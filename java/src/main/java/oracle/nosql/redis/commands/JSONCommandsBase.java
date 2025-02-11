package oracle.nosql.redis.commands;

import static oracle.nosql.redis.util.Utils.byteBufToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.JsonParseException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.commands.jsonpath.JSONPathToSQLVisitor;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathLexer;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

abstract class JSONCommandsBase extends CommandsBase {

    protected static final String ROOT_PATH = "$";
    protected static final String ROOT_PATH_PFX = "$.";
    protected static final String SQL_ROOT = "$r.value.data";
    private static final String SQL_ROOT_FILTER_FMT =
        "$r.value.values($key = 'data' AND (%s))";
    protected static final String ARRAY_CONV_KEY = "v";
    protected static final String VALUE_PAD = "pad";
    protected static final String FLD_NUM_UPD = "NumRowsUpdated";
    protected static final String FLD_RES = "res";
    protected static final String KEY_ID_COND = "$r.id = $keyId ";
    protected static final String IS_TYPE_JSON =
        String.format("AND ($r.value.type = '%s')", TYPE_JSON);
    protected static final String SQL_JSON_COND =
        KEY_ID_COND + AND_NOT_EXPIRED + IS_TYPE_JSON;
    protected static final String SQL_VAL = "$val";
    protected static final String SQL_DECLARE = "DECLARE ";
    protected static final String DECL_KEY_ID = " $keyId STRING; ";
    protected static final String DECL_KEY_ID_VAL =
        DECL_KEY_ID + SQL_VAL + " JSON; ";
    protected static final String SQL_RETURNING = " RETURNING ";
    protected static final String SQL_RETURNING_PAD = SQL_RETURNING +
        "$r.value.pad AS res";
    protected static final String VAL_FILTER_FMT = "$value IS OF TYPE (%s)";
    protected static final String MAP_FILTER = String.format(VAL_FILTER_FMT,
        "Map(Any)");
    protected static final String ARR_FILTER = String.format(VAL_FILTER_FMT,
        "Array(Any)");
    protected static final String NUM_FILTER = String.format(VAL_FILTER_FMT,
        "Number");

    protected static final String ERR_KEY_NOT_EXISTS =
        "key doesn't exist or of wrong type";
    protected static String ERR_NOT_ARRAY = "result is not an array";

    public static final String CMD_JSON_SET = "JSON.SET";
    public static final String CMD_JSON_GET = "JSON.GET";
    public static final String CMD_JSON_ARRAPPEND = "JSON.ARRAPPEND";
    public static final String CMD_JSON_ARRINSERT = "JSON.ARRINSERT";
    public static final String CMD_JSON_ARRPOP = "JSON.ARRPOP";
    public static final String CMD_JSON_ARRTRIM = "JSON.ARRTRIM";
    public static final String CMD_JSON_ARRLEN = "JSON.ARRLEN";
    public static final String CMD_JSON_ARRINDEX = "JSON.ARRINDEX";
    public static final String CMD_JSON_STRAPPEND = "JSON.STRAPPEND";
    public static final String CMD_JSON_STRLEN = "JSON.STRLEN";
    public static final String CMD_JSON_NUMINCRBY = "JSON.NUMINCRBY";
    public static final String CMD_JSON_NUMMULTBY = "JSON.NUMMULTBY";
    public static final String CMD_JSON_TOGGLE = "JSON.TOGGLE";
    public static final String CMD_JSON_TYPE = "JSON.TYPE";
    public static final String CMD_JSON_OBJKEYS = "JSON.OBJKEYS";
    public static final String CMD_JSON_OBJLEN = "JSON.OBJLEN";
    public static final String CMD_JSON_DEL = "JSON.DEL";
    public static final String CMD_JSON_FORGET = "JSON.FORGET";
    public static final String CMD_JSON_CLEAR = "JSON.CLEAR";

    protected static class TranslateResult {
        final String sqlPath;
        final String parentSQLPath;
        final Map<String, FieldValue> vars;

        static final TranslateResult ROOT_INSTANCE =
            new TranslateResult(SQL_ROOT);

        // only for root path
        private TranslateResult(String sqlPath) {
            this(sqlPath, "", Collections.emptyMap());
        }

        TranslateResult(String sqlPath, String parentSQLPath,
            Map<String, FieldValue> vars) {
            this.sqlPath = sqlPath;
            this.parentSQLPath = parentSQLPath;
            this.vars = vars;
        }

        // var1 JSON; var2 JSON; ...; varN JSON;
        String getSQLDecl() {
            return "DECLARE " +
                vars.keySet().stream().map(var -> var + " JSON;")
                .collect(Collectors.joining("  "));
        }
    }

    protected static class TranslateResultWithFilters extends TranslateResult {
        final String[] sqlPathsWithFilter;
        final String[] parentSQLPathsWithFilter;

        // only for root path
        private TranslateResultWithFilters(String sqlPath,
            String[] sqlPathsWithFilter) {
            this(sqlPath, "", sqlPathsWithFilter, null,
            Collections.emptyMap());
        }

        TranslateResultWithFilters(String sqlPath, String parentSQLPath,
            String[] sqlPathsWithFilter, String[] parentSQLPathsWithFilter,
            Map<String, FieldValue> vars) {
            super(sqlPath, parentSQLPath, vars);
            this.sqlPathsWithFilter = sqlPathsWithFilter;
            this.parentSQLPathsWithFilter = parentSQLPathsWithFilter;
        }

        static TranslateResultWithFilters rootPath(
            String[] endValueFilters) {
            return new TranslateResultWithFilters(SQL_ROOT,
                Arrays.stream(endValueFilters).map(val -> String.format(
                    SQL_ROOT_FILTER_FMT, val)).toArray(String[]::new));
        }

        // The following are used when using only one filter (which is in most
        // of the cases).

        String sqlPathWithFilter() {
            assert(sqlPathsWithFilter != null &&
                sqlPathsWithFilter.length == 1);
            return sqlPathsWithFilter[0];
        }

        String parentSQLPathWithFilter() {
            assert(parentSQLPathsWithFilter != null &&
            parentSQLPathsWithFilter.length == 1);
            return parentSQLPathsWithFilter[0];
        }
    }

    protected static class TranslateMultiResult extends TranslateResult {
        TranslateMultiResult(String[] parts, Map<String, FieldValue> vars) {
            // TranslateMultiResult is only used for GET, so parentSQLPath is
            // N/A.
            super(getSQLPath(parts), "", vars);
        }

        private static String getSQLPath(String[] parts) {
            return '[' + String.join("], [", parts) + ']';
        }
    }

    protected static class TranslateSetResult extends TranslateResult {
        final List<String> putFields;

        TranslateSetResult(JSONPathToSQLVisitor visitor) {
            super(visitor.getSQLPath(), visitor.getParentSQLPath(),
                visitor.getVariables());
            this.putFields = visitor.getPutFields();
        }

        boolean isForPut() {
            return putFields != null;
        }

        // Object with new fields to use in the PUT clause of the UPDATE
        // statement.
        // { field1: $var, field2: $var, ... }
        String getSQLNewFieldsExpr() {
            assert putFields != null;
            return putFields.stream().map(field -> field + ": $val")
                .collect(Collectors.joining(", ", "{ ", " }"));
        }

    }

    public JSONCommandsBase(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    protected static FieldValue byteBufToJson(ByteBuf buf)
        throws RedisResponseException {
        try {
            return FieldValue.createFromJson(
                byteBufToString(buf), null);
        } catch(JsonParseException ex) {
            throw new RedisResponseException(ex.getMessage(), ex);
        }
    }

    protected static MapValue makeJSONValue(ByteBuf buf)
        throws RedisResponseException {
        return new MapValue().put(VALUE_TYPE, TYPE_JSON)
            .put(VALUE_DATA, byteBufToJson(buf))
            .put(VALUE_PAD, JsonNullValue.getInstance());
    }

    // Convert a relative path to absolute.
    private static String canonizePath(String path) {
        if (path.isEmpty()) {
            return ROOT_PATH;
        }

        // If the path is "$" or starts with "$." or "$[", we treat it as an
        // absolute path. Otherwise it is a relative path (even if it starts
        // "$", based on behavior of Redis Stack).
        if (path.charAt(0) == '$' && (path.length() == 1 ||
            path.charAt(1) == '.' || path.charAt(1) == '[')) {
            return path;
        }
        
        // Redis stack allows path such as ["a"], which should mean $["a"].
        return path.startsWith("[") ? ROOT_PATH + path : ROOT_PATH_PFX + path;
    }

    private static ParseTree parsePath(String path) {
        JSONPathLexer lexer = new JSONPathLexer(CharStreams.fromString(
            canonizePath(path)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        JSONPathParser parser = new JSONPathParser(tokenStream);

        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                Object offendingSymbol, int line, int pos, String msg,
                RecognitionException ex) {
                throw Utils.parseException(path, pos, msg, ex);
            }
        });

        return parser.jsonpath();
    }

    protected static TranslateResult translatePath(String path) {
        // Optimization for common case.
        if (path == ROOT_PATH) {
            return TranslateResult.ROOT_INSTANCE;
        }

        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        visitor.run(parsePath(path), path);
        return new TranslateResult(visitor.getSQLPath(),
            visitor.getParentSQLPath(), visitor.getVariables());
    }

    protected static TranslateResultWithFilters translatePathWithFilters(
        String path, String[] endValueFilters, String[] parentFilters) {
        // Optimization for common case.
        if (path == ROOT_PATH) {
            return TranslateResultWithFilters.rootPath(endValueFilters);
        }

        ParseTree pt = parsePath(path);
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);

        // TODO: we need to reconsider this to use only one pass of the
        // visitor to get both unfiltered and filtered SQL paths. This will
        // also avoid the issue of duplicating bound variables.
        visitor.run(pt, path);

        String sqlPath = visitor.getSQLPath();
        String parentSQLPath = visitor.getParentSQLPath();
        
        String[] sqlPathsWithFilter = null;
        String[] parentSQLPathsWithFilter = null;
        
        if (endValueFilters != null) {
            sqlPathsWithFilter = new String[endValueFilters.length];
            for(int i = 0; i < endValueFilters.length; i++) {
                visitor.setEndValueFilter(endValueFilters[i]);
                visitor.run(pt, path);
                sqlPathsWithFilter[i] = visitor.getSQLPath();    
            }
        }
        
        if (parentFilters != null) {
            visitor.setEndValueFilter(null);
            parentSQLPathsWithFilter = new String[parentFilters.length];
            for(int i = 0; i < parentFilters.length; i++) {
                visitor.setParentFilter(parentFilters[i]);
                visitor.run(pt, path);
                parentSQLPathsWithFilter[i] = visitor.getParentSQLPath();
            }
        }

        return new TranslateResultWithFilters(sqlPath, parentSQLPath,
            sqlPathsWithFilter, parentSQLPathsWithFilter,
            visitor.getVariables());
    }

    protected static TranslateResultWithFilters translatePathWithFilter(
        String path, String endValueFilter, String parentFilter) {
        return translatePathWithFilters(path,
            endValueFilter != null ? new String[]{ endValueFilter } : null,
            parentFilter != null ? new String[]{ parentFilter } : null);
    }

    protected static TranslateResultWithFilters translatePathWithFilter(
        String path, String endValueFilter) {
        return translatePathWithFilter(path, endValueFilter, null);
    }

    protected static TranslateMultiResult translatePaths(List<String> paths) {
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        String[] sqlParts = new String[paths.size()];
        for(int i = 0; i < sqlParts.length; i++) {
            String path = paths.get(i);
            visitor.run(parsePath(path), path);
            sqlParts[i] = visitor.getSQLPath();
        }
        return new TranslateMultiResult(sqlParts, visitor.getVariables());
    }

    protected static TranslateSetResult translateSetPath(String path) {
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        visitor.setIsForSet(true);
        visitor.run(parsePath(path), path);
        return new TranslateSetResult(visitor);
    }

    protected static MapValue wrapArrElem(FieldValue val) {
        return new MapValue(1).put(ARRAY_CONV_KEY, val);
    }

    // JSON Path contains selectors that have the same syntax for arrays and
    // objects, including wildcard and filter expressions. In SQL systanx,
    // however, we have to distinguish between arrays and maps. Because we
    // have no knowledge of the data when we create SQL statement, we cannot
    // directly transform JSON Path expression to SQL expression.
    // The following modification takes advantage of the fact that in SQL,
    // map-filter step expressions are passed down recursively to array
    // elements and the results are concatenated, which will allow us to use
    // map-filter step expressions for both arrays and objects. For this to
    // work, we replace each array element with an object containing single
    // value. E.g. [1, 2, 3] will be replaced with
    // [{"v": 1}, {"v": 2}, {"v": 3}] (the key name does not make a
    // difference). Note that the reverse conversion also has to happen before
    // we return values to the user.
    protected static FieldValue transformValue(FieldValue val) {
        if (val.isAtomic()) {
            return val;
        }
        
        if (val.isMap()) {
            for (Map.Entry<String, FieldValue> entry :
                val.asMap().entrySet()) {
                entry.setValue(transformValue(entry.getValue()));
            }
            return val;
        }

        assert val.isArray();
        ArrayValue arr = val.asArray();
        for(int i = 0; i < arr.size(); i++) {
            arr.set(i, wrapArrElem(transformValue(arr.get(i))));
        }

        return val;
    }

    // Transform arrays to original form before returning results to the user.
    protected static FieldValue untransformValue(FieldValue val)
        throws RedisResponseException{
        if (val.isAtomic()) {
            return val;
        }
        
        if (val.isMap()) {
            for (Map.Entry<String, FieldValue> entry :
                val.asMap().entrySet()) {
                entry.setValue(untransformValue(entry.getValue()));
            }
            return val;
        }

        assert val.isArray();
        ArrayValue arr = val.asArray();
        for(int i = 0; i < arr.size(); i++) {
            FieldValue convVal = arr.get(i);
            FieldValue origVal =
                convVal.isMap() && convVal.asMap().size() == 1 ?
                convVal.asMap().get(ARRAY_CONV_KEY) : null;
            
            if (origVal == null) {
                throw RedisResponseException.corrupt(
                    "unwrapped array element");
            }
            
            arr.set(i, untransformValue(origVal));
        }

        return val;
    }

    // The result of JSON.GET operation is always an array of values that
    // match the path. We only need to untransform the values themselves, not
    // the top array.
    protected static ArrayValue untransformValues(ArrayValue vals)
        throws RedisResponseException {
        for(int i = 0; i < vals.size(); i++) {
            vals.set(i, untransformValue(vals.get(i)));
        }
        return vals;
    }

    protected static MapValue makeMultiResult(List<String> paths,
        ArrayValue vals) throws RedisResponseException {
        int cnt = paths.size();
        if (vals.size() != cnt) {
            throw RedisResponseException.corrupt(
                "JSON.GET: result count mismatch");
        }

        MapValue res = new MapValue(cnt);
        for(int i = 0; i < cnt; i++) {
            FieldValue val = vals.get(i);
            if (!val.isArray()) {
                throw RedisResponseException.corrupt(ERR_NOT_ARRAY);
            }
            res.put(paths.get(i), untransformValues(val.asArray()));
        }

        return res;
    }

    protected static int getNumRowsUpdated(MapValue row)
        throws RedisResponseException {
        int numUpd = row.getInt(FLD_NUM_UPD);
        if (numUpd != 0 && numUpd != 1) {
            throw RedisResponseException.nosql(
                "Invalid NumRowsUpdated: " + numUpd);
        }
        return numUpd;
    }

    // Returns array stored in field named "res".
    protected static ArrayValue getArrRes(MapValue row)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_RES);
        if (val == null) {
            throw RedisResponseException.nosql("Missing result field res");
        }
        if (!val.isArray()) {
            throw RedisResponseException.nosql(ERR_NOT_ARRAY);
        }
        return val.asArray();
    }

    // Returns boolean stored in field named "res".
    protected static boolean getBoolRes(MapValue row)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_RES);
        if (val == null || !val.isBoolean()) {
            throw RedisResponseException.nosql(
                "result missing or not boolean");
        }
        return val.getBoolean();
    }

    // Returns integer stored in field named "res".
    protected static int getIntRes(MapValue row)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_RES);
        if (val == null || !val.isInteger()) {
            throw RedisResponseException.nosql(
                "result missing or not integer");
        }
        return val.getInt();
    }

    protected PreparedStatement getPrepStmt(RedisKeyInfo keyInfo, String sql,
        TranslateResult tr) {
        PreparedStatement pStmt = pstmtCache.getByVal(sql);
        pStmt.setVariable("$keyId", new StringValue(keyInfo.id));
        tr.vars.forEach(
            (varName, varVal) -> pStmt.setVariable(varName, varVal));
        return pStmt;
    }

    // We need this overload when need to set additional options in
    // QueryRequest.
    protected MapValue doSQLUpdate(QueryRequest qReq,
        boolean allowEmpty) throws RedisResponseException {
        try {
            QueryResult qRes;
            do {
                qRes = nosqlHandle.query(qReq);
            } while(!qReq.isDone() && qRes.getResults().isEmpty());
            
            List<MapValue> res = qRes.getResults();
            if (allowEmpty && res.isEmpty()) {
                return null;
            }
            if (res.size() != 1) {
                throw RedisResponseException.nosql(
                    "Invalid number of results from single row update: " +
                    res.size());
            }

            return res.get(0);
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
        finally {
            qReq.close();
        }
    }

    protected MapValue doSQLUpdate(PreparedStatement pStmt,
        boolean allowEmpty) throws RedisResponseException {
        QueryRequest qReq = new QueryRequest();
        qReq.setPreparedStatement(pStmt);
        return doSQLUpdate(qReq, allowEmpty);
    }

    protected MapValue doSQLUpdate(PreparedStatement pStmt)
        throws RedisResponseException {
        return doSQLUpdate(pStmt, true);
    }

    protected MapValue doSQLGet(PreparedStatement pStmt)
        throws RedisResponseException {
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            do {
                QueryResult qRes = nosqlHandle.query(qReq);
                if (qRes.getResults().isEmpty()) {
                    continue;
                }
                if (qRes.getResults().size() > 1 || !qReq.isDone()) {
                    throw RedisResponseException.corrupt(
                        ERR_NO_SINGLE_RES);
                }
                return qRes.getResults().get(0);
            } while(!qReq.isDone());
            // The key does not exist.
            return null;
        }
    }

    protected RedisMessage getIntArrayReply(MapValue row)
        throws RedisResponseException {
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }
    
        ArrayValue arrVal = getArrRes(row);

        List<RedisMessage> res = new ArrayList<>();
        for(FieldValue val : arrVal) {
            if (val.isAnyNull()) {
                res.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                continue;
            }

            if (!val.isInteger()) {
                throw RedisResponseException.nosql(
                    "result is not integer or null");
            }

            res.add(new IntegerRedisMessage(val.getInt()));
        }

        return new ArrayRedisMessage(res);
    }

}
