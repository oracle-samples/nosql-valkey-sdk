/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import oracle.nosql.driver.RowSizeLimitException;
import oracle.nosql.valkey.RedisServerConfig;
import oracle.nosql.valkey.util.JSONDeserializer;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.JsonParseException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.valkey.RedisResponseException;
import oracle.nosql.valkey.RedisResponseException.ErrorPrefix;
import oracle.nosql.valkey.commands.jsonpath.JSONPathToSQLVisitor;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathLexer;
import oracle.nosql.valkey.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.valkey.util.PreparedStatementCache;
import oracle.nosql.valkey.util.Utils;

import static oracle.nosql.valkey.util.Utils.*;

abstract class JSONCommandsBase extends CommandsBase {

    protected static final String ROOT_PATH = "$";
    protected static final String ROOT_PATH_PFX = "$.";
    protected static final String IS_TYPE_JSON =
        String.format("$r.value.type = '%s'", TYPE_JSON);
    protected static final String SQL_ROOT_PARENT = "$r.value";
    // We use field "json" to store JSON values instead of "data". Note that
    // we should not use field "json" for any other Redis type. This way we
    // can make sure JSON operation will not clash with any other keys without
    // having to use an additional filter for every operation.
    protected static final String SQL_ROOT = SQL_ROOT_PARENT + ".json";
    protected static final String ARRAY_CONV_KEY = "v";
    protected static final String VALUE_JSON = "json";
    protected static final String VALUE_PAD = "pad";
    protected static final String FLD_IS_JSON = "isJSON";
    protected static final String AND_IS_JSON = "AND (EXISTS $r.value.json)";
    protected static final String SQL_IS_JSON =
        ", (EXISTS $r.value.json) AS isJSON";
    protected static final String SQL_EXISTS_COND =
        R_PK_COND + AND_NOT_EXPIRED;
    protected static final String SQL_VAL = "$val";
    protected static final String DECL_KEY_ID_VAL =
        DECL_KEY_ID + SQL_VAL + " JSON; ";
    protected static final String SQL_RETURNING_PAD = SQL_RETURNING +
        "$r.value.pad AS res";
    protected static final String VAL_FILTER_FMT = "$value IS OF TYPE (%s)";
    protected static final String MAP_FILTER = String.format(VAL_FILTER_FMT,
        "Map(Any)");
    protected static final String ARR_FILTER = String.format(VAL_FILTER_FMT,
        "Array(Any)");
    protected static final String NUM_FILTER = String.format(VAL_FILTER_FMT,
        "Number");

    protected static final String ERR_NEW_VAL_NOT_ROOT =
        "new objects must be created at the root";
    protected static final String ERR_KEY_NOT_EXISTS =
        "could not perform this operation on a key that doesn't exist";
    protected static final String ERR_WRONG_TYPE =
        "Existing key has wrong Redis type";
    protected static final String ERR_NOT_ARRAY = "result is not an array";
    protected static final String ERR_UNWRAPPED_ARR_ELEM =
        "unwrapped array element";

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
    public static final String CMD_JSON_MGET = "JSON.MGET";
    public static final String CMD_JSON_MERGE = "JSON.MERGE";
    public static final String CMD_JSON_MSET = "JSON.MSET";

    protected static class PathInfo {
        final String path;
        final String origPath;
        final boolean isRoot;
        final boolean isLegacy;

        static final PathInfo ROOT = new PathInfo(ROOT_PATH, ROOT_PATH, true,
            false);
        static final PathInfo LEGACY_ROOT = new PathInfo(ROOT_PATH, ".", true,
            true);

        private PathInfo(String path, String origPath, boolean isRoot,
            boolean isLegacy) {
            this.path = path;
            this.origPath = origPath;
            this.isRoot = isRoot;
            this.isLegacy = isLegacy;
        }

        static PathInfo get(String path) throws RedisResponseException {
            if (path.isEmpty()) {
                throw new RedisResponseException(
                    "Path cannot be empty string");
            }

            char c = path.charAt(0);
            boolean isRoot = path.length() == 1 && (c == '$' || c == '.');

            // JSONPath should either be "$" or start with "$." or "$[".
            // Otherwise, it is a legacy path. Note that if path starts with
            // "$" but not followed by "." or "[", it is a legacy path and "$"
            // is not root but part of a property name.
            boolean isLegacy = (c != '$') || (path.length() != 1 &&
                (path.charAt(1) != '.' && path.charAt(1) != '['));

            String oldPath = path;
            // Legacy syntax allows path such as ["a"] or .a, which should mean
            // $["a"] and $.a correspondingly.
            if (isLegacy) {
                if (path.startsWith(".")) {
                    // per Redis, allow paths starting with ".["
                    path = ROOT_PATH +
                        ((path.length() > 1 && path.charAt(1) == '[') ?
                            path.substring(1) : path);
                } else if (path.startsWith("[")) {
                    path = ROOT_PATH + path;
                } else {
                    path = ROOT_PATH_PFX + path;
                }
            }

            return new PathInfo(path, oldPath, isRoot, isLegacy);
        }

        static PathInfo get(ByteBuf path) throws RedisResponseException {
            return get(Utils.byteBufToString(path));
        }
    }

    protected static class TranslateResult {
        final String sqlPath;
        final String parentSQLPath;
        final Map<String, FieldValue> vars;
        final List<String> leafFields; // only for JSON.SET and JSON.MERGE

        // We don't use leaf fields for root path because root path always
        // exists, so there are no use cases in JSON.SET and JSON.MERGE.
        static final TranslateResult ROOT_INSTANCE =
            new TranslateResult(SQL_ROOT, SQL_ROOT_PARENT,
                Collections.emptyMap(), null);

        TranslateResult(String sqlPath, String parentSQLPath,
            Map<String, FieldValue> vars, List<String> leafFields) {
            this.sqlPath = sqlPath;
            this.parentSQLPath = parentSQLPath;
            this.vars = vars;
            this.leafFields = leafFields;
        }

        // var1 JSON; var2 JSON; ...; varN JSON;
        String getSQLDecl() {
            if (vars.isEmpty()) {
                return "";
            }

            return vars.keySet().stream().map(var -> var + " JSON;")
                .collect(Collectors.joining("  ", "", " "));
        }

        // Object with new fields to use in the PUT clause of the UPDATE
        // statement.
        // { field1: $var, field2: $var, ... }
        String getSQLNewFieldsExpr() {
            assert leafFields != null;
            return leafFields.stream().map(field -> field + ": $val")
                .collect(Collectors.joining(", ", "{ ", " }"));
        }
    }

    protected static class TranslateResultWithFilters extends TranslateResult {
        final String[] sqlPathsWithFilter;
        final String[] parentSQLPathsWithFilter;

        TranslateResultWithFilters(String sqlPath, String parentSQLPath,
            String[] sqlPathsWithFilter, String[] parentSQLPathsWithFilter,
            Map<String, FieldValue> vars, List<String> leafFields) {
            super(sqlPath, parentSQLPath, vars, leafFields);
            this.sqlPathsWithFilter = sqlPathsWithFilter;
            this.parentSQLPathsWithFilter = parentSQLPathsWithFilter;
        }

        // There is no need to use parent filters for now, but this may
        // change. We don't use leafFields for the same reason as in
        // ROOT_INSTANCE.
        static TranslateResultWithFilters rootPath(String[] endValueFilters) {
            return new TranslateResultWithFilters(SQL_ROOT, SQL_ROOT_PARENT,
                endValueFilters != null ? Arrays.stream(endValueFilters)
                    .map(val -> SQL_ROOT_PARENT +
                    String.format(".values($key = 'json' AND (%s))", val))
                    .toArray(String[]::new) : null,
                null, Collections.emptyMap(), null);
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
            super(getSQLPath(parts), "", vars, null);
        }

        private static String getSQLPath(String[] parts) {
            return '[' + String.join("], [", parts) + ']';
        }
    }

    public JSONCommandsBase(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    private static RedisResponseException emptyLegacyRes(PathInfo pi) {
        // This should not happen unless the key doesn't exist.
        if (pi.isRoot) {
            return RedisResponseException.corrupt(
                "Value at root path not found");
        }
        return new RedisResponseException(
            RedisResponseException.ErrorPrefix.ERR, String.format(
            "Path '%s' doesn't exist", pi.path));
    }

    protected static FieldValue byteBufToJson(ByteBuf buf)
        throws RedisResponseException {
        String str;
        try {
            str = byteBufToString(buf);
        } catch(Exception ex) {
            // To conform to Redis error message in this case.
            throw new RedisResponseException(
                "Couldn't parse as UTF-8 string", ex);
        }

        return JSONDeserializer.deserialize(str);
    }

    protected static MapValue makeJSONValue(FieldValue val)
        throws RedisResponseException {
        return new MapValue().put(VALUE_TYPE, TYPE_JSON)
            .put(VALUE_JSON, val)
            .put(VALUE_PAD, JsonNullValue.getInstance());
    }

    protected static FieldValue chkLegacyRes(PathInfo pi, ArrayValue res)
        throws RedisResponseException {
        if (!pi.isLegacy) {
            return res;
        }

        if (res.size() == 0) {
            throw emptyLegacyRes(pi);
        }

        return res.get(0);
    }

    protected static FieldValue chkLegacyResNoThrow(PathInfo pi,
        ArrayValue res) {
        return !pi.isLegacy ? res : (res.size() != 0 ? res.get(0) : null);
    }

    protected static RedisMessage chkLegacyRes(PathInfo pi,
        ArrayRedisMessage res) throws RedisResponseException {
        if (!pi.isLegacy) {
            return res;
        }

        List<RedisMessage> ls = res.children();

        if (ls.isEmpty()) {
            throw emptyLegacyRes(pi);
        }

        return ls.get(0);
    }

    protected static RedisMessage chkLegacyResNoThrow(PathInfo pi,
        ArrayRedisMessage res) {
        if (!pi.isLegacy) {
            return res;
        }

        List<RedisMessage> ls = res.children();
        return !ls.isEmpty() ?
            ls.get(0) : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    protected static ParseTree parsePath(String path) {
        JSONPathLexer lexer = new JSONPathLexer(CharStreams.fromString(path));
        BaseErrorListener errListener = new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                Object offendingSymbol, int line, int pos, String msg,
                RecognitionException ex) {
                throw Utils.parseException(path, pos, msg, ex);
            }
        };

        lexer.removeErrorListeners();
        lexer.addErrorListener(errListener);

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        JSONPathParser parser = new JSONPathParser(tokenStream);

        parser.removeErrorListeners();
        parser.addErrorListener(errListener);

        return parser.jsonpath();
    }

    protected static TranslateResult translatePath(PathInfo pi) {
        // Optimization for common case.
        if (pi.isRoot) {
            return TranslateResult.ROOT_INSTANCE;
        }

        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        visitor.run(parsePath(pi.path), pi.path);
        return new TranslateResult(visitor.getSQLPath(),
            visitor.getParentSQLPath(), visitor.getVariables(),
            visitor.getLeafFields());
    }

    protected static TranslateResultWithFilters translatePathWithFilters(
        PathInfo pi, String[] endValueFilters, String[] parentFilters) {
        // Optimization for common case.
        if (pi.isRoot) {
            // We can't put parent filter on root path.
            assert parentFilters == null;
            return TranslateResultWithFilters.rootPath(endValueFilters);
        }

        ParseTree pt = parsePath(pi.path);
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);

        // TODO: we need to reconsider this to use only one pass of the
        // visitor to get both unfiltered and filtered SQL paths. This will
        // also avoid the issue of duplicating bound variables.
        visitor.run(pt, pi.path);

        String sqlPath = visitor.getSQLPath();
        String parentSQLPath = visitor.getParentSQLPath();
        
        String[] sqlPathsWithFilter = null;
        String[] parentSQLPathsWithFilter = null;
        
        if (endValueFilters != null) {
            sqlPathsWithFilter = new String[endValueFilters.length];
            for(int i = 0; i < endValueFilters.length; i++) {
                visitor.setEndValueFilter(endValueFilters[i]);
                visitor.run(pt, pi.path);
                sqlPathsWithFilter[i] = visitor.getSQLPath();    
            }
        }
        
        if (parentFilters != null) {
            visitor.setEndValueFilter(null);
            parentSQLPathsWithFilter = new String[parentFilters.length];
            for(int i = 0; i < parentFilters.length; i++) {
                visitor.setParentFilter(parentFilters[i]);
                visitor.run(pt, pi.path);
                parentSQLPathsWithFilter[i] = visitor.getParentSQLPath();
            }
        }

        return new TranslateResultWithFilters(sqlPath, parentSQLPath,
            sqlPathsWithFilter, parentSQLPathsWithFilter,
            visitor.getVariables(), visitor.getLeafFields());
    }

    protected static TranslateResultWithFilters translatePathWithFilter(
        PathInfo pi, String endValueFilter, String parentFilter) {
        return translatePathWithFilters(pi,
            endValueFilter != null ? new String[]{ endValueFilter } : null,
            parentFilter != null ? new String[]{ parentFilter } : null);
    }

    protected static TranslateResultWithFilters translatePathWithFilter(
        PathInfo pi, String endValueFilter) {
        return translatePathWithFilter(pi, endValueFilter, null);
    }

    protected static TranslateMultiResult translatePaths(List<PathInfo> paths) {
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        String[] sqlParts = new String[paths.size()];
        for(int i = 0; i < sqlParts.length; i++) {
            PathInfo pi = paths.get(i);
            if (pi.isRoot) {
                sqlParts[i] = SQL_ROOT;
            } else {
                visitor.run(parsePath(pi.path), pi.path);
                sqlParts[i] = visitor.getSQLPath();
            }
        }
        return new TranslateMultiResult(sqlParts, visitor.getVariables());
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
                throw RedisResponseException.corrupt(ERR_UNWRAPPED_ARR_ELEM);
            }
            
            arr.set(i, untransformValue(origVal));
        }

        return val;
    }

    // The result of JSON.GET and JSON.MGET operations is always an array of
    // values that match the path. We only need to untransform the values
    // themselves, not the top array.
    protected static ArrayValue untransformValues(ArrayValue vals)
        throws RedisResponseException {
        for(int i = 0; i < vals.size(); i++) {
            vals.set(i, untransformValue(vals.get(i)));
        }
        return vals;
    }

    protected static void chkIsJSON(MapValue row)
        throws RedisResponseException {
        FieldValue isJSON = row.get(FLD_IS_JSON);
        if (isJSON == null || !isJSON.isBoolean()) {
            throw RedisResponseException.nosql(
                "result missing or not boolean");
        }
        if (!isJSON.getBoolean()) {
            // Using the same as in Redis Stack.
            // For some reason Redis JSON uses different error message here
            // than for other types and without WRONGTYPE prefix.
            throw new RedisResponseException(ERR_WRONG_TYPE);
        }
    }

    // Returns array stored in field named "res".
    protected static ArrayValue getArrRes(MapValue row)
        throws RedisResponseException {
        return getArrField(row, FLD_RES);
    }

    // Returns boolean stored in field named "res".
    protected static boolean getBoolRes(MapValue row)
        throws RedisResponseException {
        return getBoolField(row, FLD_RES);
    }

    // Returns integer stored in field named "res".
    protected static int getIntRes(MapValue row)
        throws RedisResponseException {
        return getIntField(row, FLD_RES);
    }

    protected PreparedStatement getPrepStmt(RedisKeyInfo keyInfo, String sql,
        TranslateResult tr) {
        PreparedStatement pStmt = pstmtCache.getByVal(sql);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
        tr.vars.forEach(
            (varName, varVal) -> pStmt.setVariable(varName, varVal));
        return pStmt;
    }

    protected RedisResponseException processNoSQLException(Exception ex) {
        // If provided or retrieved (via embedded json path) regular
        // expression pattern is unsupported or invalid, we want to avoid
        // returning raw nosql error and signal the user that the problem is
        // with the regular expression.
        if (ex instanceof IllegalArgumentException) {
            String msg = ex.getMessage();
            if (msg.contains("regex_like") ||
                msg.contains("regular expression")) {
                return new RedisResponseException(
                    "unsupported or invalid regular expression", ex);
            }
        } else if (ex instanceof RowSizeLimitException) {
            return new RedisResponseException(
                "json value exceeds maximum allowed size", ex);
        }

        return super.processNoSQLException(ex);
    }

    // We need this overload when need to set additional options in
    // QueryRequest.
    protected MapValue doSQLUpdate(QueryRequest qReq)
        throws RedisResponseException {
        try {
            QueryResult qRes;
            do {
                qRes = nosqlHandle.query(qReq);
            } while(!qReq.isDone() && qRes.getResults().isEmpty());
            
            List<MapValue> res = qRes.getResults();
            if (res.isEmpty()) {
                return null;
            }
            if (res.size() != 1) {
                throw RedisResponseException.nosql(
                    "Invalid number of results from single row update: " +
                    res.size());
            }

            return res.get(0);
        } catch(Exception ex) {
            throw processNoSQLException(ex);
        }
        finally {
            qReq.close();
        }
    }

    protected MapValue doSQLUpdate(PreparedStatement pStmt)
        throws RedisResponseException {
        QueryRequest qReq = new QueryRequest();
        qReq.setPreparedStatement(pStmt);
        return doSQLUpdate(qReq);
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
        } catch(Exception ex) {
            throw processNoSQLException(ex);
        }
    }

    protected ArrayRedisMessage getIntArrayReply(MapValue row)
        throws RedisResponseException {
        if (row == null) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                ERR_KEY_NOT_EXISTS);
        }
    
        chkIsJSON(row);
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
