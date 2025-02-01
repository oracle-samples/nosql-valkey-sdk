package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
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
    private static final String SQL_RETURNING = " RETURNING ";
    private static final String ARR_FILTER = "$value IS OF TYPE (Array(Any))";
    private static final String SQL_ARR_LENS_FMT =
        "[seq_transform(%s, CASE WHEN $ IS OF TYPE (Array(Any)) " +
        "THEN size($) ELSE NULL END)] AS res";
    private static final String SQL_ARR_APP_INS_FMT =
        "UPDATE redis $r ADD %s %s %s WHERE " + SQL_JSON_COND + SQL_RETURNING +
        SQL_ARR_LENS_FMT;
    private static final String SQL_SEL_ARR_POP_FMT =
        DECL_KEY_ID + SQL_POS_LONG +
        "SELECT row_version($r) AS ver, [seq_transform(%s, CASE WHEN $ " +
        "IS OF TYPE (Array(Any)) THEN $[%s].v ELSE NULL END)] AS res FROM " +
        "redis $r WHERE " + SQL_JSON_COND;
    private static final String SQL_UPD_REMOVE =
        "UPDATE redis $r REMOVE %s[%s] WHERE ";
    private static final String SQL_ARR_POP_FMT =
        DECL_KEY_ID + SQL_POS_LONG + "$ver BINARY; " +
        SQL_UPD_REMOVE + "row_version($r) = $ver AND " + SQL_JSON_COND;
    private static final String SQL_ARR_TRIM_FMT =
        DECL_KEY_ID + SQL_START_STOP_LONG + SQL_UPD_REMOVE + SQL_JSON_COND +
        SQL_RETURNING + SQL_ARR_LENS_FMT;
    private static final String SQL_SEL_ARR_LENS_FMT = DECL_KEY_ID +
        "SELECT " + SQL_ARR_LENS_FMT + " FROM redis $r WHERE " +
        SQL_JSON_COND;
    private static final String SQL_SEL_ARR_INDEX_OF_FMT =
        "SELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Array(Any)) THEN " +
        "index_of(concat(seq_transform($[%s], CASE WHEN $.v = $val THEN 1 " +
        "ELSE 0 END)), 1%s) ELSE NULL END)] AS res FROM redis $r WHERE " +
        SQL_JSON_COND;

    private static final String SQL_POS_EXPR_FMT =
        "(CASE WHEN %s >= 0 THEN %s ELSE size($) + %s END)";
    private static final String SQL_START_EXPR =
        String.format(SQL_POS_EXPR_FMT, SQL_START, SQL_START, SQL_START);
    private static final String SQL_STOP_EXPR =
        String.format(SQL_POS_EXPR_FMT, SQL_STOP, SQL_STOP, SQL_STOP);
    private static final String SQL_TRIM_FILTER =
        String.format("$pos < %s OR $pos > %s", SQL_START_EXPR,
        SQL_STOP_EXPR);

    private static final String ERR_KEY_NOT_EXISTS =
        "key doesn't exist or of wrong type";

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

    private RedisMessage getIntArrayReply(MapValue row)
        throws RedisResponseException {
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }
    
        ArrayValue arrVal = getArrRes(row);

        List<RedisMessage> res = new ArrayList<>();
        for(FieldValue val : arrVal) {
            if (val.isAnyNull()) {
                res.add(FullBulkStringRedisMessage.NULL_INSTANCE);
            } else {
                if (!val.isInteger()) {
                    throw RedisResponseException.nosql(
                        "result is not integer or null");
                }
                res.add(new IntegerRedisMessage(val.getInt()));
            }
        }

        return new ArrayRedisMessage(res);
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
        String sql = tr.getSQLDecl() + DECL_KEY_ID_VAL +
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

        return getIntArrayReply(doSQLUpdate(pStmt, true));
    }

    // It seems that when using SQL UPDATE REMOVE clause, there is no way to
    // return the items that have been removed, which is needed for
    // JSON.ARRPOP command. So we have to do read-modify-write sequence.
    // First we read the items about to be removed (see pSelStmt).
    // If key does not exist/is of wrong type or if there are no items to be
    // removed (the paths referenced by JSON path are not arrays or empty
    // arrays), then we can throw/return immediately. Otherwise we perform the
    // update (see pUpdStmt) and check the number of NumRowsUpdated (1 or 0).
    // The only way that NumRowsUpdated = 0 is if the row has been
    // concurrently modified by another client. Note that we condition the
    // update on the row version we updated during read. In case of version
    // mismatch, we retry the whole op up to certain number of times as done
    // for strings and collections.
    private List<RedisMessage> doJSONArrPop(PreparedStatement pSelStmt,
        PreparedStatement pUpdStmt) throws RedisResponseException {
        for(int i = 0; i < ATOMIC_SET_TRIES; i++) {
            MapValue row = doSQLGet(pSelStmt);
            if (row == null) {
                throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
            }

            FieldValue verVal = rowToVerVal(row);
            ArrayValue arrVal = untransformValues(getArrRes(row));

            List<RedisMessage> res = new ArrayList<>();
            boolean hasUpdates = false;
            
            for(FieldValue val: arrVal) {
                if (val.isEMPTY()) {
                    throw RedisResponseException.corrupt(
                        "Empty value in result set");
                }
                if (val.isNull()) {
                    res.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                } else {
                    hasUpdates = true;
                    res.add(new FullBulkStringRedisMessage(
                        Utils.stringToByteBuf(val.toJson(null))));
                }
            }

            if (!hasUpdates) {
                return res;
            }

            pUpdStmt.setVariable("$ver", verVal);
            int numUpd = getNumRowsUpdated(doSQLUpdate(pUpdStmt, false));
            if (numUpd != 0) {
                return res;
            }
        }

        throw failedAtomicRetries();   
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

        TranslateResultWithFilter tr = translatePathWithFilter(path,
            ARR_FILTER);
        String selSql = tr.getSQLDecl() + String.format(SQL_SEL_ARR_POP_FMT,
            tr.sqlPath, pos >= 0 ? SQL_POS : SQL_NEG_POS);
        String updSql = tr.getSQLDecl() + String.format(SQL_ARR_POP_FMT,
            tr.sqlPathWithFilter, pos >= 0 ? SQL_POS : SQL_NEG_POS);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        PreparedStatement pSelStmt = getPrepStmt(keyInfo, selSql, tr);
        PreparedStatement pUpdStmt = getPrepStmt(keyInfo, updSql, tr);
        pSelStmt.setVariable(SQL_POS, posVal);
        pUpdStmt.setVariable(SQL_POS, posVal);

        return new ArrayRedisMessage(doJSONArrPop(pSelStmt, pUpdStmt));
    }

    public RedisMessage handleJSONArrTrim(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 4);
        String path = Utils.byteBufToString(cmd.args[1]);
        long start = Utils.byteBufToLong(cmd.args[2]);
        long stop = Utils.byteBufToLong(cmd.args[3]);

        TranslateResultWithFilter tr = translatePathWithFilter(path,
            ARR_FILTER);
        String sql = tr.getSQLDecl() + String.format(SQL_ARR_TRIM_FMT,
            tr.sqlPathWithFilter, SQL_TRIM_FILTER, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(
            makeRedisKeyInfo(cmd.args[0]), sql, tr);
        pStmt.setVariable(SQL_START, new LongValue(start));
        pStmt.setVariable(SQL_STOP, new LongValue(stop));

        return getIntArrayReply(doSQLUpdate(pStmt, true));
    }

    public RedisMessage handleJSONArrLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException{
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        TranslateResult tr = translatePath(path);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            tr.getSQLDecl() + String.format(SQL_SEL_ARR_LENS_FMT, tr.sqlPath),
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
            tr.getSQLDecl() + DECL_KEY_ID_VAL +
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
