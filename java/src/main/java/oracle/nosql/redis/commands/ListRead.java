/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class ListRead extends ListCommandsBase {

    private static final String SQL_LRANGE_FMT = DECL_KEY_ID +
        "$var2 INTEGER; $var3 LONG; SELECT $l.value AS elemVal " +
        FROM_JOIN_WHERE_L_PK + AND_NOT_EXPIRED +
        "ORDER BY %s LIMIT $var2 OFFSET $var3";

    private static final String SQL_LRANGE =
        String.format(SQL_LRANGE_FMT, PK_COLS);
    private static final String SQL_LRANGE_DESC =
        String.format(SQL_LRANGE_FMT, PK_COLS_DESC);

    private static final String SQL_LPOS_FMT = DECL_KEY_ID +
        "%sSELECT $l.value AS elemVal%s " +
        FROM_JOIN_WHERE_L_PK + AND_NOT_EXPIRED + "ORDER BY %s%s";
    private static final String SQL_LPOS = String.format(SQL_LPOS_FMT, "", "",
        PK_COLS, "");
    private static final String SQL_LPOS_MAXLEN = String.format(SQL_LPOS_FMT,
        VAR2_LONG, "", PK_COLS, LIMIT_VAR2);
    private static final String SQL_LPOS_DESC = String.format(SQL_LPOS_FMT,
        "", VAL_LEN, PK_COLS_DESC, "");
    private static final String SQL_LPOS_DESC_MAXLEN = String.format(
        SQL_LPOS_FMT, VAR2_LONG, VAL_LEN, PK_COLS_DESC, LIMIT_VAR2);

    private static class LPosState {
        private final String elemVal;
        private final ArrayList<Long> matches;
        private long rank;
        private long len = -1;
        private long pos = -1;
        private final int count;
        private final boolean isDesc;

        LPosState(ByteBuf elemVal, long rank, int count, boolean isDesc) {
            assert elemVal != null;
            this.elemVal = makeStrVal(elemVal);
            this.count = count;
            matches = count != 0 ?
                new ArrayList<>((int)count) : new ArrayList<>();
            this.isDesc = isDesc;
            this.rank = isDesc ? -rank : rank;
        }

        // Returns true if we have count matches, otherwise false.
        boolean applyRow(MapValue row) throws RedisResponseException {
            pos++;

            if (isDesc) {
                if (len == -1) {
                    len = valToLen(row);
                }

                if (pos >= len) {
                    throw RedisResponseException.corrupt(
                        "Invalid list length: " + len);
                }
            }

            if (elemVal.equals(rowToElemVal(row))) {
                if (rank == 1) {
                    // For descending order, we are moving backwards, so we
                    // have to reverse the match position.
                    matches.add(isDesc ? len - 1 - pos : pos);
                } else {
                    rank--;
                }
                if (count != 0 && matches.size() == count) {
                    return true;
                }
            }

            return false;
        }

        ArrayList<Long> getMatches() {
            return matches;
        }

        boolean isListEmpty() {
            return pos == -1;
        }
    }

    public ListRead(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    private List<RedisMessage> doLRange(RedisKeyInfo keyInfo, long off,
        int len, boolean isDesc) throws RedisResponseException {
        List<RedisMessage> res = doQuery(keyInfo,
            isDesc ? SQL_LRANGE_DESC : SQL_LRANGE, val ->
                new FullBulkStringRedisMessage(getStrVal(rowToElemVal(val))),
                    new LongValue(len), new LongValue(off));
        if (res.isEmpty()) {
            // If the query returns no results, we have to check if the key
            // is of wrong type (not a list) to return correct error message.
            // Unfortunately, since we cannot do parent table to child table
            // LOJ (see comments in queryListElems()), we have to issue Get
            // request (doGetList will try to instantiate list header and thus
            // check the key type). Similar checks are done in functions below.
            doGetList(keyInfo);
        }
        return res;
    }

    private RedisMessage doLPos(RedisKeyInfo keyInfo, ByteBuf val, long rank,
        int count, long maxLen) throws RedisResponseException {
        boolean isDesc = rank < 0;
        boolean hasCount = count != -1;
        if (!hasCount) {
            count = 1;
        }
        String sql = !isDesc ? (maxLen == 0 ? SQL_LPOS : SQL_LPOS_MAXLEN) :
            (maxLen == 0 ? SQL_LPOS_DESC : SQL_LPOS_DESC_MAXLEN);
        LPosState lpState = new LPosState(val, rank, count, isDesc);
        if (maxLen == 0) {
            processQuery(keyInfo, sql, (row) -> lpState.applyRow(row));
        } else {
            processQuery(keyInfo, sql, (row) -> lpState.applyRow(row),
                new LongValue(maxLen));
        }

        List<Long> matches = lpState.getMatches();
        int numMatches = matches.size();
        if (numMatches == 0) {
            if (lpState.isListEmpty()) {
                doGetList(keyInfo); // wrong type check
            }
            return hasCount ?
                ArrayRedisMessage.EMPTY_INSTANCE :
                FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        if (hasCount) {
            assert count == 0 || numMatches <= count;
            ArrayList<RedisMessage> ls = new ArrayList<>(matches.size());
            matches.forEach((elem) -> ls.add(new FullBulkStringRedisMessage(
                Utils.longToByteBuf(elem))));
            return new ArrayRedisMessage(ls);
        }

        assert numMatches == 1;
        return new FullBulkStringRedisMessage(
            Utils.longToByteBuf(matches.get(0)));
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_LLEN, this::handleLLen);
        cmdMap.put(CMD_LINDEX, this::handleLIndex);
        cmdMap.put(CMD_LRANGE, this::handleLRange);
        cmdMap.put(CMD_LPOS, this::handleLPos);
    }

    public RedisMessage handleLLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return new IntegerRedisMessage(doLLen(makeRedisKeyInfo(cmd.args[0])));
    }

    public RedisMessage handleLIndex(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        long idx = Utils.byteBufToLong(cmd.args[1]);
        boolean isDesc = idx < 0;
        if (isDesc) {
            idx = -idx - 1;
        }

        List<RedisMessage> res = doLRange(makeRedisKeyInfo(cmd.args[0]),
            idx, 1, isDesc);
        return !res.isEmpty() ?
            res.get(0) : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleLRange(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        long start = Utils.byteBufToLong(cmd.args[1]);
        long stop = Utils.byteBufToLong(cmd.args[2]);
        
        // index to start with (from the end for reverse tranversal)
        long startWith;
        boolean isDesc; // whether to do forward or reverse traversal

        // If the signs of start and stop are the same, we can do either
        // forward or reverse traversal and we don't need to query for the
        // length of the list.
        if ((start >= 0 && stop >= 0) || (start < 0 && stop < 0)) {
            if (stop < start) { // holds whether start, stop are + or -
                return ArrayRedisMessage.EMPTY_INSTANCE;
            }
            isDesc = start < 0;
            if (isDesc) {
                startWith = -stop - 1;
            } else {
                startWith = start;
            }
        } else {
            // If signs of start and stop are different, we have to know the
            // length of the list.
            long len = doLLen(keyInfo);

            // For simplicity, first convert to forward indexes.
            if (start < 0) {
                start = Math.max(len + start, 0);
            }
            if (stop < 0) {
                stop = len + stop;
                // If stop is before beginning of list, the result is empty.
                if (stop < 0) {
                    return ArrayRedisMessage.EMPTY_INSTANCE;
                }
            } else {
                stop = Math.min(stop, len - 1);
            }
            if (start >= len || stop < start) {
                return ArrayRedisMessage.EMPTY_INSTANCE;
            }

            // To optimize, we do forward or reverse traversal depending on
            // whether either end of the range is closer to beginning or end
            // of the list.
            isDesc = start > len - stop - 1;
            startWith = isDesc ? len - stop - 1 : start;
        }

        List<RedisMessage> res = doLRange(keyInfo, startWith,
            (int)(stop - start) + 1, isDesc);
        if (isDesc) {
            Collections.reverse(res);
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleLPos(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkNumArgs(cmd, 2, 8);
        // must have even number of args
        if ((cmd.args.length & 1) != 0) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        long rank = 1;
        // -1 to differentiate the case where count was not provided
        long count = -1;
        long maxLen = 0;

        for(int i = 2; i < cmd.args.length; i += 2) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("RANK")) {
                rank = Utils.byteBufToLong(cmd.args[i + 1]);
                if (rank == 0) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "RANK can't be zero: use 1 to start from the first " +
                        "match, 2 from the second ... or use negative to " +
                        "start from the end of the list");
                }
                // To conform to Redis behavior.
                if (rank == Long.MIN_VALUE) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "value is out of range, value must between " +
                        String.format("%d and %d", Long.MIN_VALUE + 1,
                            Long.MAX_VALUE));
                }
            } else if (arg.equalsIgnoreCase("COUNT")) {
                count = Utils.byteBufToLong(cmd.args[i + 1]);
                if (count < 0) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "COUNT can't be negative");
                }
                // We can only support 32-bit value of count (Java collection
                // size is an int).
                if (count > Integer.MAX_VALUE) {
                    count = Integer.MAX_VALUE;
                }
            } else if (arg.equalsIgnoreCase("MAXLEN")) {
                maxLen = Utils.byteBufToLong(cmd.args[i + 1]);
                if (maxLen < 0) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "MAXLEN can't be negative");
                }
            }
        }

        return doLPos(makeRedisKeyInfo(cmd.args[0]), cmd.args[1], rank,
            (int)count, maxLen);
    }

}
