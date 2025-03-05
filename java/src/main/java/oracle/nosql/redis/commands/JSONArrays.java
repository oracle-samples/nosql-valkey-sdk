package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONArrays extends JSONCommandsBase {

    private static final String SQL_POS = "$idx";
    private static final String SQL_START = "$start";
    private static final String SQL_STOP = "$stop";
    private static final String SQL_DECL_INT = " INTEGER; ";
    private static final String SQL_DECL_LONG = " LONG; ";
    private static final String SQL_POS_LONG = SQL_POS + SQL_DECL_LONG;
    private static final String SQL_NEG_POS = "size($) + " + SQL_POS;
    private static final String SQL_START_INT = SQL_START + SQL_DECL_INT;
    private static final String SQL_START_LONG = SQL_START + SQL_DECL_LONG;
    private static final String SQL_STOP_LONG = SQL_STOP + SQL_DECL_LONG;
    private static final String SQL_START_STOP_LONG =
        SQL_START_LONG + SQL_STOP_LONG;
    private static final String SQL_VAL_SEQ = String.format("(%s[])",
        SQL_VAL);
    private static final String SQL_ARR_LENS_FMT =
        "[seq_transform(%s, CASE WHEN $ IS OF TYPE (Array(Any)) " +
        "THEN size($) ELSE NULL END)] AS res";
    private static final String SQL_ARR_APP_INS_FMT =
        " UPDATE redis $r ADD %s %s %s WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + SQL_ARR_LENS_FMT + SQL_IS_JSON;
    private static final String SQL_ARR_POP_FMT =
        DECL_KEY_ID + SQL_POS_LONG + 
        "%sUPDATE redis $r SET $r.value.pad = [seq_transform(%s, CASE WHEN " +
        // .v because arrays are in the transformed state
        "$ IS OF TYPE (Array(Any)) THEN $[%s].v ELSE NULL END)], " +
        "REMOVE %s[%s] WHERE " + SQL_EXISTS_COND + SQL_RETURNING_PAD +
        SQL_IS_JSON;
    // What happens if after trim operation one of the path items is no longer
    // in the path, e.g. if the path has a filter condition based on some
    // element of the array and that element has been trimmed. It is not clear
    // if the result should include such items. This can be fixed by storing
    // the initial path result in the pad and then using it via seq_transform
    // to obtain the lenghts in the RETURNING clause, however this involves
    // using too much storage in the pad (all arrays in the initial path), so
    // not doing this currently.
    private static final String SQL_ARR_TRIM_FMT =
        DECL_KEY_ID + SQL_START_STOP_LONG +
        "%sUPDATE redis $r REMOVE %s[%s] WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + SQL_ARR_LENS_FMT + SQL_IS_JSON;
    private static final String SQL_SEL_ARR_LENS_FMT = DECL_KEY_ID +
        "%sSELECT " + SQL_ARR_LENS_FMT + SQL_IS_JSON +
        " FROM redis $r WHERE " + SQL_EXISTS_COND;
    private static final String SQL_SEL_ARR_INDEX_OF_FMT =
        "SELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Array(Any)) THEN " +
        "index_of(concat(seq_transform($[%s], CASE WHEN $.v = $val THEN 1 " +
        "ELSE 0 END)), 1%s) ELSE NULL END)] AS res " + SQL_IS_JSON +
        " FROM redis $r WHERE " + SQL_EXISTS_COND;

    private static final String SQL_POS_EXPR_FMT =
        "(CASE WHEN %s >= 0 THEN %s ELSE size($) + %s END)";
    private static final String SQL_START_EXPR =
        String.format(SQL_POS_EXPR_FMT, SQL_START, SQL_START, SQL_START);
    private static final String SQL_STOP_EXPR =
        String.format(SQL_POS_EXPR_FMT, SQL_STOP, SQL_STOP, SQL_STOP);
    private static final String SQL_TRIM_FILTER =
        String.format("$pos < %s OR $pos > %s", SQL_START_EXPR,
        SQL_STOP_EXPR);

    public JSONArrays(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    // Used to confine position/offset within int32 bounds, even though Redis
    // commands accept int64.
    private static int longToInt(long val) {
        if (val < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        if (val > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int)val;
    }

    private static FieldValue byteBufToArrElem(ByteBuf buf)
        throws RedisResponseException {
        return wrapArrElem(transformValue(byteBufToJson(buf)));
    }

    private RedisMessage handleArrAppendInsert(RedisClientContext client,
        RawCommand cmd, boolean isInsert) throws RedisResponseException {
        int minArgs = isInsert ? 4 : 3;
        chkMinNumArgs(cmd, minArgs);
        String path = Utils.byteBufToString(cmd.args[1]);
        long pos = isInsert ? Utils.byteBufToLong(cmd.args[2]) : -1;

        FieldValue val;
        if (cmd.args.length == minArgs) {
            val = byteBufToArrElem(cmd.args[minArgs - 1]);
        } else {
            val = new ArrayValue();
            for(int i = minArgs - 1; i < cmd.args.length; i++) {
                val.asArray().add(byteBufToArrElem(cmd.args[i]));
            }
        }

        TranslateResult tr = translatePath(path);
        String sql = DECL_KEY_ID_VAL + tr.getSQLDecl() +
            (isInsert ? SQL_POS_LONG : "") +
            String.format(SQL_ARR_APP_INS_FMT, tr.sqlPath,
            // It seems we cannot use CASE expr in position expr, so we have
            // to handle positive and negative pos separately.
            isInsert ? (pos >= 0 ? SQL_POS : SQL_NEG_POS) : "",
            cmd.args.length == minArgs ? SQL_VAL : SQL_VAL_SEQ, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_VAL, val);
        
        if (isInsert) {
            pStmt.setVariable(SQL_POS, new LongValue(pos));
        }

        return getIntArrayReply(doSQLUpdate(pStmt));
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_ARRAPPEND, this::handleJSONArrAppend);
        cmdMap.put(CMD_JSON_ARRINSERT, this::handleJSONArrInsert);
        cmdMap.put(CMD_JSON_ARRPOP, this::handleJSONArrPop);
        cmdMap.put(CMD_JSON_ARRTRIM, this::handleJSONArrTrim);
        cmdMap.put(CMD_JSON_ARRLEN, this::handleJSONArrLen);
        cmdMap.put(CMD_JSON_ARRINDEX, this::handleJSONArrIndex);
    }

    public RedisMessage handleJSONArrAppend(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleArrAppendInsert(client, cmd, false);
    }

    public RedisMessage handleJSONArrInsert(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleArrAppendInsert(client, cmd, true);
    }

    public RedisMessage handleJSONArrPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 3);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        long pos = cmd.args.length > 2 ?
            Utils.byteBufToLong(cmd.args[2]) : -1;
        LongValue posVal = new LongValue(pos);

        TranslateResultWithFilters tr = translatePathWithFilter(path,
            ARR_FILTER);
        
        String sqlPathWithFilter = tr.sqlPathWithFilter();
        String sqlPos = pos >= 0 ? SQL_POS : SQL_NEG_POS;
        
        String sql = String.format(SQL_ARR_POP_FMT, tr.getSQLDecl(),
            tr.sqlPath, sqlPos, sqlPathWithFilter, sqlPos);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_POS, posVal);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }

        chkIsJSON(row);
        ArrayValue arrVal = getArrRes(row);

        List<RedisMessage> res = new ArrayList<>();
        for(FieldValue val : arrVal) {
            res.add(val.isAnyNull() ?
                FullBulkStringRedisMessage.NULL_INSTANCE :
                new FullBulkStringRedisMessage(Utils.stringToByteBuf(
                    untransformValue(val).toJson(null))));
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleJSONArrTrim(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 4);
        String path = Utils.byteBufToString(cmd.args[1]);
        long start = Utils.byteBufToLong(cmd.args[2]);
        long stop = Utils.byteBufToLong(cmd.args[3]);

        TranslateResultWithFilters tr = translatePathWithFilter(path,
            ARR_FILTER);
        String sql = String.format(SQL_ARR_TRIM_FMT, tr.getSQLDecl(),
            tr.sqlPathWithFilter(), SQL_TRIM_FILTER, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(
            makeRedisKeyInfo(cmd.args[0]), sql, tr);
        pStmt.setVariable(SQL_START, new LongValue(start));
        pStmt.setVariable(SQL_STOP, new LongValue(stop));

        return getIntArrayReply(doSQLUpdate(pStmt));
    }

    public RedisMessage handleJSONArrLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException{
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            String.format(SQL_SEL_ARR_LENS_FMT, tr.getSQLDecl(), tr.sqlPath),
            tr);

        return getIntArrayReply(doSQLGet(pStmt));
    }

    public RedisMessage handleJSONArrIndex(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException{
        chkNumArgs(cmd, 3, 5);
        String path = Utils.byteBufToString(cmd.args[1]);
        FieldValue val = transformValue(byteBufToJson(cmd.args[2]));
        // start_position in index_of() must be integer
        IntegerValue startVal = null;
        LongValue stopVal = null;
        String startExpr = "";
        String boundExpr = "";

        if (cmd.args.length > 3) {
            startVal = new IntegerValue(longToInt(
                Utils.byteBufToLong(cmd.args[3])));
            startExpr = ", " + SQL_START_EXPR;
            if (cmd.args.length > 4) {
                long stop = Utils.byteBufToLong(cmd.args[4]);
                // in Redis JSON stop = 0 is treated as no stop value
                if (stop != 0) {
                    stopVal = new LongValue(Utils.byteBufToLong(cmd.args[4]));
                    boundExpr += "$pos < " + SQL_STOP_EXPR;
                }
            }
        }

        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            DECL_KEY_ID_VAL + tr.getSQLDecl() +
            (startVal == null ? "" : SQL_START_INT) +
            (stopVal == null ? "" : SQL_STOP_LONG) +
            String.format(SQL_SEL_ARR_INDEX_OF_FMT, tr.sqlPath, boundExpr,
                startExpr), tr);
        pStmt.setVariable(SQL_VAL, val);
        if (startVal != null) {
            pStmt.setVariable(SQL_START, startVal);
            if (stopVal != null) {
                pStmt.setVariable(SQL_STOP, stopVal);
            }
        }

        return getIntArrayReply(doSQLGet(pStmt));
    }

}
