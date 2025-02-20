package oracle.nosql.redis.commands;

import static oracle.nosql.redis.util.Utils.byteBufToString;
import static oracle.nosql.redis.util.Utils.stringToByteBuf;

import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONGetSet extends JSONCommandsBase {

    private static final String STR_FILTER = String.format(VAL_FILTER_FMT,
        "String");
    private static final String BOOL_FILTER = String.format(VAL_FILTER_FMT,
        "Boolean");
    private static final String SQL_UPDATE_SET_FMT =
        "UPDATE redis $r SET $r.value.pad = (EXISTS %s), %s = $val WHERE " +
        SQL_JSON_COND + SQL_RETURNING_PAD;
    private static final String SQL_UPDATE_PUT_FMT =
        "UPDATE redis $r PUT %s %s WHERE " + SQL_JSON_COND + SQL_RETURNING +
        "EXISTS %s AS res";
    private static final String SQL_UPDATE_PUT_NX_FMT =
        "UPDATE redis $r SET $r.value.pad = (NOT EXISTS %s), " +
        "PUT %s[NOT EXISTS %s] %s WHERE " + SQL_JSON_COND + SQL_RETURNING +
        "$r.value.pad AND EXISTS %s AS res";
    private static final String SQL_GET_FMT = DECL_KEY_ID +
        "SELECT [%s] AS res FROM redis $r WHERE " + SQL_JSON_COND;
    private static final String SQL_MGET_FMT = DECL_KEY_IDS +
        "SELECT id, [%s] AS res FROM redis $r WHERE " + KEY_IDS_COND +
        AND_NOT_EXPIRED + IS_TYPE_JSON;
    private static final String SQL_NUM_INCR_MULT_FMT = DECL_KEY_ID_VAL +
        "UPDATE redis $r SET %s = $ %c $val WHERE " + SQL_JSON_COND +
        SQL_RETURNING + "[seq_transform(%s, CASE WHEN $ IS OF TYPE " +
        "(Number) THEN $ ELSE NULL END)] AS res";
    protected static final String SQL_STR_LENS_FMT =
        "[seq_transform(%s, CASE WHEN $ IS OF TYPE (String) " +
        "THEN size($) ELSE NULL END)] AS res";
    private static final String SQL_STR_APPEND_FMT = DECL_KEY_ID_VAL +
        "UPDATE redis $r SET %s = $ || $val WHERE " + SQL_JSON_COND +
        SQL_RETURNING + SQL_STR_LENS_FMT;
    private static final String SQL_SEL_STR_LENS_FMT = DECL_KEY_ID +
        "SELECT " + SQL_STR_LENS_FMT + " FROM redis $r WHERE " +
        SQL_JSON_COND;
    private static final String SQL_BOOL_TOGGLE_FMT = DECL_KEY_ID +
        "UPDATE redis $r SET %s = NOT $ WHERE " + SQL_JSON_COND +
        SQL_RETURNING + "[seq_transform(%s, CASE WHEN $ IS OF TYPE " +
        "(Boolean) THEN (CASE WHEN $ THEN 1 ELSE 0 END) ELSE NULL END)] " +
        "AS res";
    private static final String SQL_SEL_TYPES_FMT = DECL_KEY_ID +
        "SELECT [seq_transform(%s, " +
        "CASE WHEN $ IS OF TYPE (String) THEN 'string' " +
        "WHEN $ IS OF TYPE (Long) THEN 'integer' " +
        "WHEN $ IS OF TYPE (Number) THEN 'number' " +
        "WHEN $ IS OF TYPE (Boolean) THEN 'boolean' " +
        "WHEN $ IS OF TYPE (Array(Any)) THEN 'array' " +
        "WHEN $ IS OF TYPE (Map(Any)) THEN 'object' " +
        // somehow IS NULL doesn't work here
        "WHEN $ = NULL THEN 'null' " +
        "ELSE NULL END)] AS res FROM redis $r WHERE " + SQL_JSON_COND;
    private static final String SQL_SEL_OBJ_KEYS_FMT = DECL_KEY_ID +
        "SELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Map(Any)) THEN " +
        "[$.keys()] ELSE NULL END)] AS res FROM redis $r WHERE " +
        SQL_JSON_COND;
    private static final String SQL_SEL_OBJ_LENS_FMT = DECL_KEY_ID +
        "SELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Map(Any)) THEN " +
        "size($) ELSE NULL END)] AS res FROM redis $r WHERE " +
        SQL_JSON_COND;

    public JSONGetSet(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private boolean doJSONSet(RedisKeyInfo keyInfo, String path,
        FieldValue val, SetOpt setOpt) throws RedisResponseException {
        TranslateSetResult tr = translateSetPath(path);
        
        if (setOpt == SetOpt.NX && !tr.isForPut()) {
            throw new RedisResponseException(
                "cannot create new fields with this path");
        }

        String sql = tr.getSQLDecl() + DECL_KEY_ID_VAL +
            (!tr.isForPut() || setOpt == SetOpt.XX ?
                String.format(SQL_UPDATE_SET_FMT, tr.sqlPath, tr.sqlPath) :
                (setOpt != SetOpt.NX ?
                    String.format(SQL_UPDATE_PUT_FMT, tr.parentSQLPath,
                        tr.getSQLNewFieldsExpr(), tr.sqlPath) :
                    String.format(SQL_UPDATE_PUT_NX_FMT, tr.sqlPath,
                        tr.parentSQLPath, tr.sqlPath,
                        tr.getSQLNewFieldsExpr(), tr.sqlPath)));
        
        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        pStmt.setVariable(SQL_VAL, transformValue(val));

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            throw new RedisResponseException(
                "new objects must be created at the root");
        }
        
        return getBoolRes(row);
    }

    private FieldValue doJSONGet(RedisKeyInfo keyInfo, List<String> paths)
        throws RedisResponseException {
        assert !paths.isEmpty();
        TranslateResult tr = paths.size() == 1 ?
            translatePath(paths.get(0)) : translatePaths(paths);
        String sql = tr.getSQLDecl() + String.format(SQL_GET_FMT, tr.sqlPath);

        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        
        MapValue row = doSQLGet(pStmt);
        if (row == null) {
            return null;
        }

        ArrayValue res = getArrRes(doSQLGet(pStmt));
        return paths.size() == 1 ?
            untransformValues(res) : makeMultiResult(paths, res);
    }

    private RedisMessage handleJSONNumIncrMult(RedisClientContext client,
        RawCommand cmd, char op) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        
        String path = Utils.byteBufToString(cmd.args[1]);
        FieldValue val = byteBufToJson(cmd.args[2]);

        if (!val.isNumeric()) {
            throw new RedisResponseException("bad input number");
        }
        
        TranslateResultWithFilters tr = translatePathWithFilter(path,
            NUM_FILTER);
        String sql = tr.getSQLDecl() + String.format(SQL_NUM_INCR_MULT_FMT,
            tr.sqlPathWithFilter(), op, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_VAL, val);
        
        QueryRequest qReq = new QueryRequest();
        // Avoiding one-liner because of the resource leak warning.
        qReq.setPreparedStatement(pStmt);
        qReq.setMathContext(MathContext.DECIMAL64);

        MapValue row = doSQLUpdate(qReq, true);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }

        ArrayValue arrVal = getArrRes(row);
        // Per spec, we return bulk string representing JSON array (rather
        // than ArrayRedisMessage).
        return new FullBulkStringRedisMessage(
            Utils.stringToByteBuf(arrVal.toJson(null)));
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_SET, this::handleJSONSet);
        cmdMap.put(CMD_JSON_GET, this::handleJSONGet);
        cmdMap.put(CMD_JSON_NUMINCRBY, this::handleJSONNumIncrBy);
        cmdMap.put(CMD_JSON_NUMMULTBY, this::handleJSONNumMultBy);
        cmdMap.put(CMD_JSON_STRAPPEND, this::handleJSONStrAppend);
        cmdMap.put(CMD_JSON_STRLEN, this::handleJSONStrLen);
        cmdMap.put(CMD_JSON_TOGGLE, this::handleJSONToggle);
        cmdMap.put(CMD_JSON_TYPE, this::handleJSONType);
        cmdMap.put(CMD_JSON_OBJKEYS, this::handleJSONObjKeys);
        cmdMap.put(CMD_JSON_OBJLEN, this::handleJSONObjLen);
        cmdMap.put(CMD_JSON_MGET, this::handleJSONMGet);
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
            Utils.stringToByteBuf(res.toJson(jsonOpts)));
    }

    public RedisMessage handleJSONNumIncrBy(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleJSONNumIncrMult(client, cmd, '+');
    }

    public RedisMessage handleJSONNumMultBy(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleJSONNumIncrMult(client, cmd, '*');
    }

    public RedisMessage handleJSONStrAppend(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        
        String path = Utils.byteBufToString(cmd.args[1]);
        FieldValue val = byteBufToJson(cmd.args[2]);
        
        if (!val.isString()) {
            throw new RedisResponseException("bad input string");
        }

        TranslateResultWithFilters tr = translatePathWithFilter(path,
            STR_FILTER);
        String sql = tr.getSQLDecl() + String.format(SQL_STR_APPEND_FMT,
            tr.sqlPathWithFilter(), "||", tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_VAL, val);
        
        return getIntArrayReply(doSQLUpdate(pStmt));
    }

    public RedisMessage handleJSONStrLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            tr.getSQLDecl() + String.format(SQL_SEL_STR_LENS_FMT, tr.sqlPath),
            tr);

        return getIntArrayReply(doSQLGet(pStmt));
    }

    public RedisMessage handleJSONToggle(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;

        TranslateResultWithFilters tr = translatePathWithFilter(path,
            BOOL_FILTER);
        String sql = tr.getSQLDecl() + String.format(SQL_BOOL_TOGGLE_FMT,
            tr.sqlPathWithFilter(), tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);        
        return getIntArrayReply(doSQLUpdate(pStmt));
    }

    public RedisMessage handleJSONType(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            tr.getSQLDecl() + String.format(SQL_SEL_TYPES_FMT, tr.sqlPath),
            tr);

        MapValue row = doSQLGet(pStmt);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }
    
        ArrayValue arrVal = getArrRes(row);

        List<RedisMessage> res = new ArrayList<>();
        for(FieldValue val : arrVal) {
            if (val.isAnyNull()) {
                throw RedisResponseException.corrupt(
                    "invalid data type in JSON");
            }
            if (!val.isString()) {
                throw RedisResponseException.nosql(
                    "invalid data type in array result");
            }
            res.add(new FullBulkStringRedisMessage(
                stringToByteBuf(val.getString())));
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleJSONObjKeys(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            tr.getSQLDecl() + String.format(SQL_SEL_OBJ_KEYS_FMT, tr.sqlPath),
            tr);

        MapValue row = doSQLGet(pStmt);
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

            if (!val.isArray()) {
                throw RedisResponseException.nosql(
                    "result is not array or null");
            }

            List<RedisMessage> valRes = new ArrayList<>();
            for(FieldValue key : val.asArray()) {
                if (!key.isString()) {
                    throw RedisResponseException.nosql(
                        "object key is not a string");
                }
                valRes.add(new FullBulkStringRedisMessage(
                    stringToByteBuf(key.getString())));
            }

            res.add(new ArrayRedisMessage(valRes));
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleJSONObjLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            tr.getSQLDecl() + String.format(SQL_SEL_OBJ_LENS_FMT, tr.sqlPath),
            tr);

        return getIntArrayReply(doSQLGet(pStmt));
    }

    public RedisMessage handleJSONMGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        ArrayValue keyIds = new ArrayValue().addAll(
            Arrays.stream(cmd.args, 0, cmd.args.length - 1).map(
                val -> new StringValue(makeRedisKeyInfo(val).id)));
        String path = byteBufToString(cmd.args[cmd.args.length - 1]);

        TranslateResult tr = translatePath(path);
        String sql = tr.getSQLDecl() + String.format(SQL_MGET_FMT,
            tr.sqlPath);

        PreparedStatement pStmt = pstmtCache.getByVal(sql);
        pStmt.setVariable("$keyIds", keyIds);
        tr.vars.forEach(
            (varName, varVal) -> pStmt.setVariable(varName, varVal));
        
        // We need to return results for all provided keys in order, but the
        // query results may be in different order and/or missing non-existent
        // keys or keys of wrong type, so we need to collect query results
        // first to create the final result.
        HashMap<String,ArrayValue> resMap = new HashMap<>();
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                for(MapValue row : qir) {
                    FieldValue fldId = row.get(FLD_ID);
                    if (fldId == null || !fldId.isString()) {
                        throw RedisResponseException.corrupt(
                            "Missing or invalid key id");
                    }
                    ArrayValue arrRes = getArrRes(row);
                    resMap.put(fldId.getString(), arrRes);
                }
            }
        } catch(Exception ex) {
            throw processNoSQLException(ex);
        }

        ArrayList<RedisMessage> res = new ArrayList<>();
        for(FieldValue keyId: keyIds) {
            ArrayValue arrRes = resMap.get(keyId.getString());
            res.add(arrRes != null ?
                new FullBulkStringRedisMessage(Utils.stringToByteBuf(
                    arrRes.toJson(null))) :
                FullBulkStringRedisMessage.NULL_INSTANCE);
        }

        return new ArrayRedisMessage(res);
    }

}
