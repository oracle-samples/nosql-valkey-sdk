/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.util.HashMap;
import java.util.List;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class ListTrimRemove extends ListCommandsBase {

    private static final String SQL_LREM = String.format(SQL_ELEMS_FMT,
        VAR2_STR_VAR3_INT, "", ELEM_VAL_VAR2, "", LIMIT_VAR3, "");
    private static final String SQL_LREM_DESC = String.format(SQL_ELEMS_FMT,
        VAR2_STR_VAR3_INT, "", ELEM_VAL_VAR2, DESC, LIMIT_VAR3, "");

    private static class ListTrimInfo extends ListValueInfo {
        long start;
        long stop;

        ListTrimInfo(ListValueInfo valInfo, long start, long stop)
            throws RedisResponseException {
            super(valInfo.header);
            this.start = start;
            this.stop = stop;
        }
    }

    public ListTrimRemove(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private Integer doLRem(RedisKeyInfo keyInfo, ByteBuf val, int cnt,
        boolean isDesc) throws RedisResponseException {
        return doMultiUpdate(keyInfo,
            (ki) -> queryListElems(ki, isDesc ? SQL_LREM_DESC : SQL_LREM,
                new StringValue(makeStrVal(val)), new LongValue(cnt)),
            (ki, lvi, upInfo) -> makeDeleteListElems(ki, lvi, upInfo, false),
            (header, res) -> {
                if (header == null) {
                    return 0;
                }

                List<OperationResult> opsRes = res.getResults();
                int opCnt = opsRes.size();

                // The first operation result is for list header, the rest is
                // for elements popped.
                if (opCnt < 2) {
                    throw RedisResponseException.nosql(
                        "Invalid number of delete results: " + opsRes.size());
                }

                assert opsRes.get(0).getSuccess();

                for(int i = 1; i < opCnt; i++) {
                    OperationResult opRes = opsRes.get(i);
                    if (!opRes.getSuccess()) {
                        throw RedisResponseException.nosql(
                            "Invalid unsuccessful operation result");
                    }
                }

                return Integer.valueOf(opCnt - 1);
            });
    }
    
    // Note that we have to select the elements to trim on the left size using
    // ascending query and the elements to trim on the right side using
    // descending query. This is because of Redis requirement that the cost of
    // LTRIM has to be proportional to number of elements trimmed and not the
    // whole length of the list. If, e.g. we were to select only one last
    // element to trim on the right side but used ascending query, the query
    // engine would have to iterate through the whole list before reaching
    // this element (even if using OFFSET clause), thus violating this
    // requirement.
    // We prefer to trim both ends of the list in one transaction, if
    // possible (number of elements to remove <= MAX_TXN_ELEM_CNT). If not,
    // we do the right side first then left (this choice is arbitrary).
    // To simplify subsequent calls to this function, we convert start and
    // stop to canonical form such that start is non-negative and stop is
    // negative, in this form we know number of elements to query for both
    // left and right queries (since right query is desc). This would only
    // have to be done on the first invocation.
    // Note that start and stop indicate the left and right positions of
    // the elements we keep. The elements we delete (and thus return their ids
    // from this function) are to the left of start and to the right of stop.
    // For the result, we save the earliest version and exp time of the list
    // header we get from get or query requests below (listVal). This way we
    // don't need to worry about list changing concurrently during operations
    // below. If it does change, the update performed by doMultiUpdate() will
    // fail with version mismatch and we retry on the next iteration.
    private CollectionValueResult<ListTrimInfo> queryElemsForTrim(
        RedisKeyInfo keyInfo, long start, long stop)
        throws RedisResponseException {
        CollectionValueResult<ListValueInfo> leftRes = null;
        CollectionValueResult<ListValueInfo> rightRes = null;
        ListValueInfo leftVal = null;
        ListValueInfo rightVal = null;

        long remaining = MAX_TXN_ELEM_CNT;
        CollectionValueResult<ListValueInfo> listRes = null;

        // If start < 0 and stop >= 0, we cannot do either left or right
        // query without knowing the length of the list. If start >= 0 or
        // stop < 0, we can do at least one of left/right queries and which
        // will give us the length of the list to do the other (if start == 0
        // or stop == -1, either left or right trimming is not needed so we
        // don't do corresponding query, but still need to get the list
        // length for the opposite query).
        if (start <= 0 && stop >= -1) {
            // Special case where no trimming on either side is needed.
            if (start == 0 && stop == -1) {
                return CollectionValueResult.none();
            }
            
            listRes = doGetList(keyInfo);
            if (listRes.data == null) {
                return CollectionValueResult.none();
            }

            if (start < 0) {
                start = Math.max(start + listRes.data.header.len, 0);
            }
            if (stop >= 0) {
                stop = Math.min(stop - listRes.data.header.len, -1);
            }
            // check again after conversion of start and stop
            if (start == 0 && stop == -1) {
                return CollectionValueResult.none();
            }
        }

        // Do the right side first if possible.
        if (stop < -1) {
            // Last arg is the number of elements to be deleted.
            rightRes = queryListElems(keyInfo, SQL_RPOP,
                new LongValue(Math.min(-stop - 1, remaining)));
            rightVal = rightRes.data;
            if (rightVal == null) {
                return CollectionValueResult.none();
            }

            if (listRes == null) {
                listRes = rightRes;
            }
            remaining -= rightVal.elemIds.size();

            // Convert start to canonical form.
            if (start < 0) {
                start = Math.max(start + rightRes.data.header.len, 0);
            }

            // If start is past stop, (e.g start > stop if both are positive),
            // the result should be empty list. However, if start exceeds stop
            // by more than 1 (because trimming ends before start and begins
            // after stop), left and right queries will have overlapping
            // (duplicate) results, so we adjust start accordingly.
            // Note that the Math.max() below is used because the expression
            // that calculates 1 past stop could still be negative if stop is
            // before the beginning of the list (e.g. list len = 5,
            // stop = -10, 1 past stop = -4). In this case the result should
            // be empty list and the query above will get all list elements
            // and the "if" block below will not be executed.
            start = Math.min(start,
                Math.max(rightRes.data.header.len + stop + 1, 0));

            // Update value of stop for next invocation.
            stop = Math.max(stop, -rightVal.header.len) +
                rightVal.elemIds.size();
            assert stop < 0;
        }

        // Do the left side, either because start was already in canonical
        // form or because it was converted to canonical form by getting the
        // list length in the block above.
        if (start > 0 && remaining != 0) {
            leftRes = queryListElems(keyInfo, SQL_LPOP,
                new LongValue(Math.min(start, remaining)));
            leftVal = leftRes.data;
            if (leftVal == null) {
                return CollectionValueResult.none();
            }

            if (listRes == null) {
                listRes = leftRes;
            }
            remaining -= leftVal.elemIds.size();

            // If we could not do the right side before, we can do it now.
            if (stop >= 0) {
                // Convert stop to canonical form. Math.max below shifts stop
                // if necessary to avoid overlapping results, same logic as
                // for start above.
                stop = Math.min(
                    Math.max(stop, start - 1) - leftVal.header.len, -1);
                
                assert rightRes == null;
                if (stop < -1 && remaining != 0) {
                    rightRes = queryListElems(keyInfo, SQL_RPOP,
                        new LongValue(Math.min(-stop - 1, remaining)));
                    rightVal = rightRes.data;
                    if (rightVal == null) {
                        return CollectionValueResult.none();
                    }

                    // Update value of stop for next invocation.
                    stop = Math.max(stop, -rightVal.header.len) +
                        rightVal.elemIds.size();
                }
            }

            // Update value of start for next invocation.
            start = Math.min(start, leftVal.header.len - 1) -
                leftVal.elemIds.size();
        }

        // Case of start == 0 and stop == -1 is already handled above.
        assert listRes != null;

        ListTrimInfo res = new ListTrimInfo(listRes.data, start, stop);
        
        if (leftRes != null) {
            res.elemIds.addAll(leftVal.elemIds);
        }
        if (rightRes != null) {
            res.elemIds.addAll(rightVal.elemIds);
        }
        
        return new CollectionValueResult<>(listRes.val, res);
    }

    private boolean doTrim(RedisKeyInfo keyInfo, long start, long stop)
        throws RedisResponseException {
        // Use array to allow lambda to modify outside state.
        long [] bounds = { start, stop };

        // Here success means trim was performed (there were list elements to
        // be trimmed).
        boolean success = false;
        do {
            ListTrimInfo trimRes = doMultiUpdate(keyInfo,
                (ki) -> queryElemsForTrim(keyInfo, bounds[0], bounds[1]),
                (ki, lvi, upInfo) -> makeDeleteListElems(keyInfo, lvi, upInfo,
                    false),
                (trimInfo, header, res) -> trimInfo);

            if (trimRes == null) {
                return success;
            }

            success = true;
            bounds[0] = trimRes.start;
            bounds[1] = trimRes.stop;
        } while(bounds[0] != 0 || bounds[1] != -1);

        return success;
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_LREM, this::handleLRem);
        cmdMap.put(CMD_LTRIM, this::handleLTrim);
    }

    public RedisMessage handleLRem(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        
        long cnt = Utils.byteBufToLong(cmd.args[1]);
        boolean isDesc = cnt < 0;
        if (isDesc) {
            cnt = -cnt;
        }
        if (cnt == 0) {
            cnt = Long.MAX_VALUE;
        }

        ByteBuf val = cmd.args[2];
        
        long res;
        
        if (cnt != 0 && cnt < MAX_TXN_ELEM_CNT) {
            res = doLRem(keyInfo, val, (int)cnt, isDesc);
        } else {
            res = 0;
            do {
                long numRem = Math.min(cnt, MAX_TXN_ELEM_CNT);
                long res1 = doLRem(keyInfo, val, (int)numRem, isDesc);
                res += res1;
                // We removed less than requested, which means no more of
                // elements with requested value is left in the list.
                if (res1 < numRem) {
                    break;
                }
                cnt -= res1;
            } while (cnt > 0);
        }

        return new IntegerRedisMessage(res);
    }

    public RedisMessage handleLTrim(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        doTrim(keyInfo, Utils.byteBufToLong(cmd.args[1]),
            Utils.byteBufToLong(cmd.args[2]));
        return okReply;
    }

}
