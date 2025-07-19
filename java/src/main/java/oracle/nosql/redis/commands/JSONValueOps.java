package oracle.nosql.redis.commands;

import static oracle.nosql.redis.util.Utils.stringToByteBuf;

import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONValueOps extends JSONCommandsBase {

    private static final String STR_FILTER = String.format(VAL_FILTER_FMT,
        "String");
    private static final String BOOL_FILTER = String.format(VAL_FILTER_FMT,
        "Boolean");
    private static final String SQL_NUM_INCR_MULT_FMT = DECL_KEY_ID_VAL +
        "%sUPDATE redis $r SET %s = $ %c $val WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + "[seq_transform(%s, CASE WHEN $ IS OF TYPE " +
        "(Number) THEN $ ELSE NULL END)] AS res" + SQL_IS_JSON;
    protected static final String SQL_STR_LENS_FMT =
        "[seq_transform(%s, CASE WHEN $ IS OF TYPE (String) " +
        "THEN length($) ELSE NULL END)] AS res";
    private static final String SQL_STR_APPEND_FMT = DECL_KEY_ID_VAL +
        "%sUPDATE redis $r SET %s = ($ || $val) WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + SQL_STR_LENS_FMT + SQL_IS_JSON;
    private static final String SQL_SEL_STR_LENS_FMT = DECL_KEY_ID +
        "%sSELECT " + SQL_STR_LENS_FMT + SQL_IS_JSON +
        " FROM redis $r WHERE " + SQL_EXISTS_COND;
    private static final String SQL_BOOL_TOGGLE_FMT = DECL_KEY_ID +
        "%sUPDATE redis $r SET %s = NOT $ WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + "[seq_transform(%s, CASE WHEN $ IS OF TYPE " +
        "(Boolean) THEN (CASE WHEN $ THEN 1 ELSE 0 END) ELSE NULL END)] " +
        "AS res" + SQL_IS_JSON;
    private static final String SQL_SEL_TYPES_FMT = DECL_KEY_ID +
        "%sSELECT [seq_transform(%s, " +
        "CASE WHEN $ IS OF TYPE (String) THEN 'string' " +
        "WHEN $ IS OF TYPE (Long) THEN 'integer' " +
        "WHEN $ IS OF TYPE (Number) THEN 'number' " +
        "WHEN $ IS OF TYPE (Boolean) THEN 'boolean' " +
        "WHEN $ IS OF TYPE (Array(Any)) THEN 'array' " +
        "WHEN $ IS OF TYPE (Map(Any)) THEN 'object' " +
        // somehow IS NULL doesn't work here
        "WHEN $ = NULL THEN 'null' " +
        "ELSE NULL END)] AS res" + SQL_IS_JSON + " FROM redis $r WHERE " +
        SQL_EXISTS_COND;
    private static final String SQL_SEL_OBJ_KEYS_FMT = DECL_KEY_ID +
        "%sSELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Map(Any)) THEN " +
        "[$.keys()] ELSE NULL END)] AS res" + SQL_IS_JSON +
        " FROM redis $r WHERE " + SQL_EXISTS_COND;
    private static final String SQL_SEL_OBJ_LENS_FMT = DECL_KEY_ID +
        "%sSELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Map(Any)) THEN " +
        "size($) ELSE NULL END)] AS res" + SQL_IS_JSON +
        " FROM redis $r WHERE " +  SQL_EXISTS_COND;

    public JSONValueOps(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
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
        String sql = String.format(SQL_NUM_INCR_MULT_FMT, tr.getSQLDecl(),
            tr.sqlPathWithFilter(), op, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_VAL, val);
        
        QueryRequest qReq = new QueryRequest();
        // Avoiding one-liner because of the resource leak warning.
        qReq.setPreparedStatement(pStmt);
        qReq.setMathContext(MathContext.DECIMAL64);

        MapValue row = doSQLUpdate(qReq);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }

        chkIsJSON(row);
        ArrayValue arrVal = getArrRes(row);
        // Per spec, we return bulk string representing JSON array (rather
        // than ArrayRedisMessage).
        return new FullBulkStringRedisMessage(
            Utils.stringToByteBuf(arrVal.toJson(null)));
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_NUMINCRBY, this::handleJSONNumIncrBy);
        cmdMap.put(CMD_JSON_NUMMULTBY, this::handleJSONNumMultBy);
        cmdMap.put(CMD_JSON_STRAPPEND, this::handleJSONStrAppend);
        cmdMap.put(CMD_JSON_STRLEN, this::handleJSONStrLen);
        cmdMap.put(CMD_JSON_TOGGLE, this::handleJSONToggle);
        cmdMap.put(CMD_JSON_TYPE, this::handleJSONType);
        cmdMap.put(CMD_JSON_OBJKEYS, this::handleJSONObjKeys);
        cmdMap.put(CMD_JSON_OBJLEN, this::handleJSONObjLen);
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
        String sql = String.format(SQL_STR_APPEND_FMT, tr.getSQLDecl(),
            tr.sqlPathWithFilter(), tr.sqlPath);
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
            String.format(SQL_SEL_STR_LENS_FMT, tr.getSQLDecl(), tr.sqlPath),
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
        String sql = String.format(SQL_BOOL_TOGGLE_FMT, tr.getSQLDecl(),
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
            String.format(SQL_SEL_TYPES_FMT, tr.getSQLDecl(), tr.sqlPath), tr);

        MapValue row = doSQLGet(pStmt);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }
    
        chkIsJSON(row);
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
            String.format(SQL_SEL_OBJ_KEYS_FMT, tr.getSQLDecl(), tr.sqlPath),
            tr);

        MapValue row = doSQLGet(pStmt);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }

        chkIsJSON(row);
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
            String.format(SQL_SEL_OBJ_LENS_FMT, tr.getSQLDecl(), tr.sqlPath),
            tr);

        return getIntArrayReply(doSQLGet(pStmt));
    }

}
