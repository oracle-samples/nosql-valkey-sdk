/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

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
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONArrays extends JSONCommandsBase {

    private static final String SQL_POS = "$idx";
    private static final String SQL_START = "$start";
    private static final String SQL_STOP = "$stop";
    private static final String SQL_DECL_INT = " INTEGER; ";
    private static final String SQL_DECL_LONG = " LONG; ";
    private static final String SQL_POS_LONG = SQL_POS + SQL_DECL_LONG;
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
    private static final String SQL_INS_POS_EXPR =
        "(CASE WHEN $idx >= 0 THEN $idx ELSE size($) + $idx END)";
    private static final String SQL_ARR_APP_INS_FMT =
        " UPDATE valkey $r ADD %s %s %s WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + SQL_ARR_LENS_FMT + SQL_IS_JSON;
    // For JSON.ARRINSERT we have to return error if index is out of bounds,
    // so we have to use additional filter to avoid update in this case. This
    // filter finds the path elements for which the position is out of range.
    // It works for both positive and negative indexes. See
    // handleJSONArrInsert().
    private static final String SQL_POS_OUT_OF_RANGE_FILTER = ARR_FILTER +
        " AND ($idx > size($value) OR $idx < -size($value))";
    private static final String SQL_POP_POS_EXPR =
        "(CASE WHEN $idx >= 0 AND $idx < size($) THEN $idx " +
        "WHEN $idx < 0 AND $idx >= -size($) THEN size($) + $idx " +
        "WHEN $idx >= size($) THEN size($) - 1 " +
        "ELSE 0 END)"; // $idx < -size($)
    private static final String SQL_ARR_POP_FMT =
        DECL_KEY_ID + SQL_POS_LONG + 
        "%sUPDATE valkey $r SET $r.value.pad = [seq_transform(%s, CASE WHEN " +
        // Note that we return wrapped elements, so the actual value will be
        // under "v" field of the map value returned ($[%s] in THEN below).
        "$ IS OF TYPE (Array(Any)) AND size($) != 0 THEN $[%s] " +
        "ELSE NULL END)], REMOVE %s[%s] WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING_PAD + SQL_IS_JSON;
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
        "%sUPDATE valkey $r REMOVE %s[%s] WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING + SQL_ARR_LENS_FMT + SQL_IS_JSON;
    private static final String SQL_SEL_ARR_LENS_FMT = DECL_KEY_ID +
        "%sSELECT " + SQL_ARR_LENS_FMT + SQL_IS_JSON +
        " FROM valkey $r WHERE " + SQL_EXISTS_COND;
    private static final String SQL_SEL_ARR_INDEX_OF_FMT =
        "SELECT [seq_transform(%s, CASE WHEN $ IS OF TYPE (Array(Any)) THEN " +
        "index_of(concat(seq_transform($[%s], CASE WHEN $.v = $val THEN 1 " +
        "ELSE 0 END)), 1%s) ELSE NULL END)] AS res " + SQL_IS_JSON +
        " FROM valkey $r WHERE " + SQL_EXISTS_COND;

    private static final String SQL_TRIM_POS_EXPR_FMT =
        "(CASE WHEN %s >= 0 THEN %s ELSE size($) + %s END)";
    private static final String SQL_START_EXPR =
        String.format(SQL_TRIM_POS_EXPR_FMT, SQL_START, SQL_START, SQL_START);
    private static final String SQL_STOP_EXPR =
        String.format(SQL_TRIM_POS_EXPR_FMT, SQL_STOP, SQL_STOP, SQL_STOP);
    private static final String SQL_TRIM_FILTER =
        String.format("$pos < %s OR $pos > %s", SQL_START_EXPR,
        SQL_STOP_EXPR);

    public JSONArrays(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
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
        chkMinNumArgs(cmd, 3);
        PathInfo pi = PathInfo.get(cmd.args[1]);

        FieldValue val;
        if (cmd.args.length == 3) {
            val = byteBufToArrElem(cmd.args[2]);
        } else {
            val = new ArrayValue();
            for(int i = 2; i < cmd.args.length; i++) {
                val.asArray().add(byteBufToArrElem(cmd.args[i]));
            }
        }

        TranslateResult tr = translatePath(pi);
        String sql = DECL_KEY_ID_VAL + tr.getSQLDecl() +
            String.format(SQL_ARR_APP_INS_FMT, tr.sqlPath, "",
                cmd.args.length == 3 ? SQL_VAL : SQL_VAL_SEQ, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_VAL, val);

        return chkLegacyRes(pi, getIntArrayReply(doSQLUpdate(pStmt)));
    }

    public RedisMessage handleJSONArrInsert(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 4);
        PathInfo pi = PathInfo.get(cmd.args[1]);
        long pos = Utils.byteBufToLong(cmd.args[2]);

        FieldValue val;
        if (cmd.args.length == 4) {
            val = byteBufToArrElem(cmd.args[3]);
        } else {
            val = new ArrayValue();
            for(int i = 3; i < cmd.args.length; i++) {
                val.asArray().add(byteBufToArrElem(cmd.args[i]));
            }
        }

        // JSON.ARRINSERT has to return an error if the position is out of
        // range for any of the path elements returned by the give JSON path.
        // We can't just put a filter on the path to filter out such path
        // elements since this would allow for partial updates for those path
        // elements for which the position is in range. Instead, this operation
        // has to all or nothing. To achieve this, we can use the negative
        // filter to find the paths for which the position is out of range and
        // use it in a CASE expression for the new-value expression of the ADD
        // clause to only pass new value if none of such paths exists
        // (otherwise, the CASE expression returns empty sequence and the
        // update is a no-op). For the path expression in the ADD clause and
        // in the expression in the RETURNING clause, we use original
        // translated path with no filter (ARR_FILTER is not needed since the
        // update is a no-op for path elements that are not arrays).
        TranslateResultWithFilters tr = translatePathWithFilter(pi,
            SQL_POS_OUT_OF_RANGE_FILTER);
        String valExpr = String.format(
            "(CASE WHEN NOT EXISTS(%s) THEN %s END)", tr.sqlPathWithFilter(),
            cmd.args.length == 4 ? SQL_VAL : SQL_VAL_SEQ);
        String sql = DECL_KEY_ID_VAL + tr.getSQLDecl() + SQL_POS_LONG +
            String.format(SQL_ARR_APP_INS_FMT, tr.sqlPath, SQL_INS_POS_EXPR,
                valExpr, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);
        pStmt.setVariable(SQL_VAL, val);
        pStmt.setVariable(SQL_POS, new LongValue(pos));

        ArrayRedisMessage res = getIntArrayReply(doSQLUpdate(pStmt));

        // Since the query already returns the new array lengths, we can just
        // check the position out of range condition here rather than putting
        // addition filter into the query. If position was in range for all
        // arrays in the path before update, this will still hold true after
        // successful update (if not, the update is a no-op).
        if (res.children().stream().anyMatch(elem -> {
            if (!(elem instanceof IntegerRedisMessage)) {
                return false;
            }
            long len = ((IntegerRedisMessage)elem).value();
            return pos > len || pos < -len;
        })) {
            throw new RedisResponseException(
                RedisResponseException.ErrorPrefix.ERR, "index out of bounds");
        }

        return chkLegacyRes(pi, res);
    }

    public RedisMessage handleJSONArrPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 3);
        PathInfo pi = cmd.args.length > 1 ?
            PathInfo.get(cmd.args[1]) : PathInfo.LEGACY_ROOT;
        long pos = cmd.args.length > 2 ?
            Utils.byteBufToLong(cmd.args[2]) : -1;
        LongValue posVal = new LongValue(pos);

        TranslateResultWithFilters tr = translatePathWithFilter(pi,
            ARR_FILTER);
        
        String sqlPathWithFilter = tr.sqlPathWithFilter();

        String sql = String.format(SQL_ARR_POP_FMT, tr.getSQLDecl(),
            tr.sqlPath, SQL_POP_POS_EXPR, sqlPathWithFilter, SQL_POP_POS_EXPR);
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
            // NULL can mean that either the path element is not an array or
            // it is an empty array.
            if (val.isAnyNull()) {
                res.add(FullBulkStringRedisMessage.NULL_INSTANCE);
            } else {
                // The reason we don't return the elements already unwrapped
                // (using .v in the query) is to be able to distinguish
                // between the case when the path is not an array or an empty
                // array (the "if" above) and the case when an array element
                // itself is null, in which case we still return it's JSON
                // representation.
                FieldValue elem = null;
                if (val.isMap()) {
                    elem = val.asMap().get(ARRAY_CONV_KEY);
                }
                if (elem == null) {
                    throw RedisResponseException.corrupt(
                        ERR_UNWRAPPED_ARR_ELEM);
                }
                res.add(new FullBulkStringRedisMessage(Utils.stringToByteBuf(
                    untransformValue(elem).toJson(null))));
            }
        }

        return chkLegacyRes(pi, new ArrayRedisMessage(res));
    }

    public RedisMessage handleJSONArrTrim(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 4);
        PathInfo pi = PathInfo.get(cmd.args[1]);
        long start = Utils.byteBufToLong(cmd.args[2]);
        long stop = Utils.byteBufToLong(cmd.args[3]);

        TranslateResultWithFilters tr = translatePathWithFilter(pi,
            ARR_FILTER);
        String sql = String.format(SQL_ARR_TRIM_FMT, tr.getSQLDecl(),
            tr.sqlPathWithFilter(), SQL_TRIM_FILTER, tr.sqlPath);
        PreparedStatement pStmt = getPrepStmt(
            makeRedisKeyInfo(cmd.args[0]), sql, tr);
        pStmt.setVariable(SQL_START, new LongValue(start));
        pStmt.setVariable(SQL_STOP, new LongValue(stop));

        return chkLegacyRes(pi, getIntArrayReply(doSQLUpdate(pStmt)));
    }

    public RedisMessage handleJSONArrLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException{
        chkNumArgs(cmd, 1, 2);
        PathInfo pi = cmd.args.length > 1 ?
            PathInfo.get(cmd.args[1]) : PathInfo.LEGACY_ROOT;
        
        TranslateResult tr = translatePath(pi);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            String.format(SQL_SEL_ARR_LENS_FMT, tr.getSQLDecl(), tr.sqlPath),
            tr);

        return chkLegacyRes(pi, getIntArrayReply(doSQLGet(pStmt)));
    }

    public RedisMessage handleJSONArrIndex(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException{
        chkNumArgs(cmd, 3, 5);
        PathInfo pi = PathInfo.get(cmd.args[1]);
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

        TranslateResult tr = translatePath(pi);
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

        return chkLegacyRes(pi, getIntArrayReply(doSQLGet(pStmt)));
    }

}
