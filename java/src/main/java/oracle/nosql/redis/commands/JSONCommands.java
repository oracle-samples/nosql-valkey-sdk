package oracle.nosql.redis.commands;

import static oracle.nosql.redis.util.Utils.byteBufToString;
import static oracle.nosql.redis.util.Utils.stringToByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.JsonParseException;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.commands.jsonpath.JSONPathToSQLVisitor;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathLexer;
import oracle.nosql.redis.commands.jsonpath.parser.JSONPathParser;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONCommands extends CommandsBase {

    private static final String ROOT_PATH = "$";
    private static final String ROOT_PATH_PFX = "$.";
    private static final String SQL_ROOT = "r.value.data";
    private static final String ARRAY_CONV_KEY = "v";
    private static final String FLD_NUM_UPD = "NumRowsUpdated";

    private static final String SQL_UPDATE_SET_FMT =
        "UPDATE redis r SET %s = $val WHERE r.id = $keyId";
    private static final String SQL_UPDATE_PUT_FMT =
        "UPDATE redis r PUT %s%s %s WHERE r.id = $keyId";

    private static final String SQL_GET_FMT =
        "SELECT [%s] AS v FROM redis r WHERE r.id = $keyId";

    private static String ERR_NOT_ARRAY = "JSON.GET result is not an array";

    public static final String CMD_JSON_SET = "JSON.SET";
    public static final String CMD_JSON_GET = "JSON.GET";
    public static final String CMD_JSON_STRAPPEND = "JSON.STRAPPEND";
    public static final String CMD_JSON_STRLEN = "JSON.STRLEN";
    public static final String CMD_JSON_NUMINCRBY = "JSON.NUMINCRBY";
    public static final String CMD_JSON_NUMMULTBY = "JSON.NUMMULTBY";
    public static final String CMD_JSON_DEL = "JSON.DEL";

    private static class TranslateResult {
        final String sqlPath;
        final Map<String, FieldValue> vars;

        static final TranslateResult ROOT_INSTANCE =
            new TranslateResult(SQL_ROOT, Collections.emptyMap());

        TranslateResult(String sqlPath, Map<String, FieldValue> vars) {
            this.sqlPath = sqlPath;
            this.vars = vars;
        }

        // var1 JSON; var2 JSON; ...; varN JSON;
        String getSQLDecl() {
            return vars.keySet().stream().map(var -> var + " JSON;")
                .collect(Collectors.joining("  "));
        }
    }

    private static class TranslateMultiResult extends TranslateResult {
        TranslateMultiResult(String[] parts, Map<String, FieldValue> vars) {
            // TranslateMultiResult is only used for GET, so putFields is N/A
            super(getSQLPath(parts), vars);
        }

        private static String getSQLPath(String[] parts) {
            return '[' + String.join("], [", parts) + ']';
        }
    }

    private static class TranslateSetResult extends TranslateResult {
        final String sqlPutPath;
        final List<String> putFields;

        TranslateSetResult(JSONPathToSQLVisitor visitor) {
            super(visitor.getSQLPath(), visitor.getVariables());
            this.sqlPutPath = visitor.getSQLPutPath();
            this.putFields = visitor.getPutFields();
        }

        // Object with new fields to use in the PUT clause of the UPDATE
        // statement. 
        // { field1: $var, field2: $var, ... }
        String getSQLNewFieldsExpr() {
            assert putFields != null;
            return putFields.stream().map(field -> field + ": $val")
                .collect(Collectors.joining(", ", "{ ", " }"));
        }

        // [NOT EXISTS $.values($key = field1 OR $key = field2 OR ...)]
        String getSQLNXFilter() {
            return String.format("[NOT EXISTS $.values(%s)]",
                putFields.stream().map(field -> "$key = " + field)
                    .collect(Collectors.joining(" OR ")));
        }
    }

    public JSONCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private static FieldValue byteBufToJson(ByteBuf buf)
        throws RedisResponseException {
        try {
            return FieldValue.createFromJson(
                byteBufToString(buf), null);
        } catch(JsonParseException ex) {
            throw new RedisResponseException(ex.getMessage(), ex);
        }
    }

    private static MapValue makeJSONValue(ByteBuf buf)
        throws RedisResponseException {
        return new MapValue().put(VALUE_TYPE, TYPE_JSON)
            .put(VALUE_DATA, byteBufToJson(buf));
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

    private static TranslateResult translatePath(String path) {
        // Optimization for common case.
        if (path == ROOT_PATH) {
            return TranslateResult.ROOT_INSTANCE;
        }

        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        visitor.run(parsePath(path), path);
        return new TranslateResult(visitor.getSQLPath(),
            visitor.getVariables());
    }

    private static TranslateMultiResult translatePaths(List<String> paths) {
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT);
        String[] sqlParts = new String[paths.size()];
        for(int i = 0; i < sqlParts.length; i++) {
            String path = paths.get(i);
            visitor.run(parsePath(path), path);
            sqlParts[i] = visitor.getSQLPath();
        }
        return new TranslateMultiResult(sqlParts, visitor.getVariables());
    }

    private static TranslateSetResult translateSetPath(String path) {
        JSONPathToSQLVisitor visitor = new JSONPathToSQLVisitor(SQL_ROOT,
            true);
        visitor.run(parsePath(path), path);
        return new TranslateSetResult(visitor);
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
    private static FieldValue transformValue(FieldValue val) {
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
            arr.set(i, new MapValue(1).put(ARRAY_CONV_KEY,
                transformValue(arr.get(i))));
        }

        return val;
    }

    // Transform arrays to original form before returning results to the user.
    private static FieldValue untransformValue(FieldValue val)
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
                throw RedisResponseException.corrupt("");
            }
            
            arr.set(i, untransformValue(origVal));
        }

        return val;
    }

    // The result of JSON.GET operation is always an array of values that
    // match the path. We only need to untransform the values themselves, not
    // the top array.
    private static ArrayValue untransformValues(ArrayValue vals)
        throws RedisResponseException {
        for(int i = 0; i < vals.size(); i++) {
            vals.set(i, untransformValue(vals.get(i)));
        }
        return vals;
    }

    private static MapValue makeMultiResult(List<String> paths,
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

    private static int getNumRowsUpdated(QueryResult qRes)
        throws RedisResponseException {
        List<MapValue> res = qRes.getResults();
        if (res.size() != 1) {
            throw RedisResponseException.nosql(
                "Invalid number of results from UPDATE: " + res.size());
        }

        int numUpd = res.get(0).getInt(FLD_NUM_UPD);
        if (numUpd != 0 && numUpd != 1) {
            throw RedisResponseException.nosql(
                "Invalid NumRowsUpdated: " + numUpd);
        }

        return numUpd;
    }

    private boolean doJSONSet(RedisKeyInfo keyInfo, String path,
        FieldValue val, SetOpt setOpt) throws RedisResponseException {
        TranslateSetResult tr = translateSetPath(path);
        String sql = "DECLARE $keyId STRING; $val JSON; " + tr.getSQLDecl() +
            (tr.sqlPutPath == null || setOpt == SetOpt.XX ?
                String.format(SQL_UPDATE_SET_FMT, tr.sqlPath) :
                String.format(SQL_UPDATE_PUT_FMT, tr.sqlPutPath,
                    setOpt == SetOpt.NX ? tr.getSQLNXFilter() : "",
                    tr.getSQLNewFieldsExpr()));
        
        PreparedStatement pStmt = pstmtCache.getByVal(sql);
        
        pStmt.setVariable("$keyId", new StringValue(keyInfo.id));
        pStmt.setVariable("$val", transformValue(val));
        tr.vars.forEach(
            (varName, varVal) -> pStmt.setVariable(varName, varVal));
        
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            QueryResult qRes;
            do {
                qRes = nosqlHandle.query(qReq);
            } while(!qReq.isDone() && qRes.getResults().isEmpty());
            return getNumRowsUpdated(qRes) == 1;
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    private FieldValue doJSONGet(RedisKeyInfo keyInfo, List<String> paths)
        throws RedisResponseException {
        assert !paths.isEmpty();
        TranslateResult tr = paths.size() == 1 ?
            translatePath(paths.get(0)) : translatePaths(paths);
        String sql = "DECLARE $keyId STRING; " + tr.getSQLDecl() +
            String.format(SQL_GET_FMT, tr.sqlPath);

        PreparedStatement pStmt = pstmtCache.getByVal(sql);    
        pStmt.setVariable("$keyId", new StringValue(keyInfo.id));
        tr.vars.forEach(
            (varName, varVal) -> pStmt.setVariable(varName, varVal));
        
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            do {
                QueryResult qRes = nosqlHandle.query(qReq);
                if (!qRes.getResults().isEmpty()) {
                    if (qRes.getResults().size() > 1 || !qReq.isDone()) {
                        throw RedisResponseException.corrupt(
                            ERR_NO_SINGLE_RES);
                    }
                    FieldValue val = qRes.getResults().get(0).get("v");
                    if (!val.isArray()) {
                        throw RedisResponseException.corrupt(ERR_NOT_ARRAY);
                    }
                    return paths.size() == 1 ?
                        untransformValues(val.asArray()) :
                        makeMultiResult(paths, val.asArray());
                }
            } while(!qReq.isDone());
            // The key does not exist.
            return null;
        }
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_SET, this::handleJSONSet);
        cmdMap.put(CMD_JSON_GET, this::handleJSONGet);
        //cmdMap.put(CMD_JSON_STRAPPEND, this::handleJSONStrAppend);
        //cmdMap.put(CMD_JSON_STRLEN, this::handleJSONStrLen);
        //cmdMap.put(CMD_JSON_NUMINCRBY, this::handleJSONIncrBy);
        //cmdMap.put(CMD_JSON_NUMMULTBY, this::handleJSONMultBy);
        //cmdMap.put(CMD_JSON_DEL, this::handleJSONDel);
    }

    public RedisMessage handleJSONSet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 3, 4);
        
        String path = Utils.byteBufToString(cmd.args[1]);
        ByteBuf val = cmd.args[2];
        SetOpt setOpt = null;

        if (cmd.args.length == 4) {
            String arg = Utils.byteBufToString(cmd.args[3]);
            if (arg.equalsIgnoreCase("NX")) {
                chkNotSet(setOpt);
                setOpt = SetOpt.NX;
            } else if (arg.equalsIgnoreCase("XX")) {
                chkNotSet(setOpt);
                setOpt = SetOpt.XX;
            } else {
                throw RedisResponseException.syntaxError();
            }
        }

        // With root path, we may either set new value or overwrite existing
        // value, so we use doGetSet() approach.
        if (path.equals(ROOT_PATH)) {
            final SetOpt setOpt1 = setOpt; // copy to use in doGetSet()
            return doGetSet(
                cmd.args[0],
                (oldVal) -> setOpt1 == null ||
                    (setOpt1 == SetOpt.NX && !oldVal.exists()) ||
                    (setOpt1 == SetOpt.XX && oldVal.exists()),
                (oldVal) -> {
                    if (oldVal.val != null &&
                        !getValueType(oldVal.val).equals(TYPE_JSON)) {
                        throw RedisResponseException.wrongType();
                    }
                    return new RedisValueInfo(makeJSONValue(val), null,
                        oldVal.exp);
                },
                // newVal is NONE if SET is not successful
                (oldVal, newVal) -> {
                    assert(setOpt1 != null || newVal.exists());
                    return newVal.exists() ?
                        okReply : FullBulkStringRedisMessage.NULL_INSTANCE;
                }, true);
        }

        // With any other path, we can only update existing value. To do this
        // in 1 request (vs doing GET and PUT), we use JSONPathToSQL
        // translator to construct an update statement.
        return doJSONSet(makeRedisKeyInfo(cmd.args[0]), path,
            byteBufToJson(val), setOpt) ?
            okReply : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleJSONGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        
        ArrayList<String> paths = new ArrayList<>();
        String indent = null;
        String newLine = null;
        String space = null;

        for(int i = 1; i < cmd.args.length; i++) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("INDENT")) {
                chkMinNumArgs(cmd, i + 2);
                indent = Utils.byteBufToString(cmd.args[++i]);
            } else if (arg.equalsIgnoreCase("NEWLINE")) {
                chkMinNumArgs(cmd, i + 2);
                newLine = Utils.byteBufToString(cmd.args[++i]);
            } if (arg.equalsIgnoreCase("SPACE")) {
                chkMinNumArgs(cmd, i + 2);
                space = Utils.byteBufToString(cmd.args[++i]);
            } else {
                paths.add(arg);
            }
        }

        if (paths.isEmpty()) {
            paths.add(ROOT_PATH);
        }

        FieldValue res = doJSONGet(makeRedisKeyInfo(cmd.args[0]), paths);
        if (res == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        // TODO: fully support formatting options above.
        JsonOptions jsonOpts =
            indent != null || newLine != null || space != null ?
            JsonOptions.PRETTY : null;
        return new FullBulkStringRedisMessage(
            stringToByteBuf(res.toJson(jsonOpts)));
    }

}
