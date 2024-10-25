/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class ListPushPop extends ListCommandsBase {

    private static final String LEFT = "LEFT";
    private static final String RIGHT = "RIGHT";
    private static final String COUNT = "COUNT";

    private static final int MIN_DELAY_MS = 200;
    private static final int MAX_ADD_RND_DELAY_MS = 50;
    private static final int MAX_DELAY_MS = 5000;

    public ListPushPop(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private ListHeader doLRPush(RedisKeyInfo keyInfo, ByteBuf[] elems,
        int off, int cnt, boolean isLeft, boolean ifExists)
        throws RedisResponseException {
        assert cnt > 0;
        return doMultiUpdate(keyInfo, (ki) -> queryListElems(ki,
                isLeft ? SQL_LPUSH : SQL_RPUSH),
            (ki, lvi, upInfo) -> {
                ListHeader header = lvi != null ? lvi.header : null;

                BigDecimal startId;
                if (header != null) {
                    assert lvi.elemIds.size() == 1;
                    if (isLeft) {
                        startId = lvi.elemIds.get(0)
                            .setScale(0, RoundingMode.HALF_DOWN)
                            .subtract(BigDecimal.ONE);
                    } else {
                        startId = lvi.elemIds.get(0)
                            .setScale(0, RoundingMode.HALF_UP)
                            .add(BigDecimal.ONE);
                    }
                    header.len += cnt;
                } else { // The list doesn't exist or expired.
                    if (ifExists) {
                        return null;
                    }
                    startId = BigDecimal.ZERO;
                    // Create new list with cnt elements.
                    header = new ListHeader(cnt);
                }

                upInfo.addPutKeyReq(header);
                
                int end = off + cnt;
                for(int i = off; i < end; i++) {
                    upInfo.addElemReq(makePutElemReq(ki, startId, header.cid,
                        elems[i]));
                    startId = isLeft ? startId.subtract(BigDecimal.ONE) :
                        startId.add(BigDecimal.ONE);
                }

                return header;
            }, (header, res) -> header);
    }

    private RedisMessage handleLRPush(RedisClientContext client,
        RawCommand cmd, boolean isLeft, boolean ifExists)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        int elemCnt = cmd.args.length - 1;
        ListHeader header;

        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        if (elemCnt <= MAX_TXN_ELEM_CNT) {
            header = doLRPush(keyInfo, cmd.args, 1, elemCnt, isLeft,
                ifExists);
        } else {
            int i = 0;
            do {
                int cnt = Math.min(MAX_TXN_ELEM_CNT, cmd.args.length - i);
                header = doLRPush(keyInfo, cmd.args, i, cnt, isLeft,
                    ifExists);
                i += MAX_TXN_ELEM_CNT;
            } while(i < cmd.args.length && header != null);
        }

        // doLRPush can return null only for LPUSHX and RPUSHX
        assert header != null || ifExists;
        return new IntegerRedisMessage(header != null ? header.len : 0);
    }

    private List<RedisMessage> doLRPop(RedisKeyInfo keyInfo, int cnt,
        boolean isLeft) throws RedisResponseException {
        assert cnt > 0;
        return doMultiUpdate(keyInfo,
            (ki) -> queryListElems(ki, isLeft ? SQL_LPOP : SQL_RPOP,
                new LongValue(cnt)),
            (ki, lvi, upInfo) -> makeDeleteListElems(ki, lvi, upInfo, true),
            (header, res) -> {
                if (res == null) {
                    return null;
                }

                List<OperationResult> opsRes = res.getResults(); 
                if (opsRes.size() < 2) {
                    throw RedisResponseException.nosql(
                        "Invalid number of delete results: " + opsRes.size());
                }
                int resCnt = opsRes.size() - 1;
                List<RedisMessage> vals = new ArrayList<>(resCnt);

                // The first operation result is for list header, the rest is
                // for elements popped.
                assert opsRes.get(0).getSuccess();

                for(int i = 0; i < resCnt; i++) {
                    OperationResult opRes = opsRes.get(i + 1);
                    if (!opRes.getSuccess()) {
                       throw RedisResponseException.nosql(
                            "Invalid unsuccessful operation result");
                    }
                    MapValue val = opRes.getExistingValue();
                    if (val == null) {
                        throw RedisResponseException.nosql(
                            "Missing existing value in operation result");
                    }
                    vals.add(new FullBulkStringRedisMessage(
                        getStrVal(rowToElemVal(val))));
                }

                return vals;
            });
    }

    // Pass cnt == -1 if count is not specified, otherwise cnt must be
    // positive.
    private RedisMessage handleLRPop(RedisClientContext client,
        ByteBuf keyBuf, int cnt, boolean isLeft)
        throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);

        if (cnt <= MAX_TXN_ELEM_CNT) {
            // Mimics the behavior observed with Redis server when count = 0.
            if (cnt == 0) {
                return doLLen(keyInfo) == 0 ?
                    FullBulkStringRedisMessage.NULL_INSTANCE :
                    ArrayRedisMessage.EMPTY_INSTANCE;
            }

            List<RedisMessage> res = doLRPop(keyInfo, cnt == -1 ? 1 : cnt,
                isLeft);
            if (res == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            
            assert !res.isEmpty();
            if (cnt == -1) {
                assert res.size() == 1;
                return res.get(0);
            }
            return new ArrayRedisMessage(res);
        } else {
            List<RedisMessage> allRes = new ArrayList<>();
            do {
                int numPop = Math.min(MAX_TXN_ELEM_CNT, cnt);
                List<RedisMessage> res = doLRPop(keyInfo, numPop, isLeft);
                if (res == null) {
                    break;
                }
                allRes.addAll(res);
                if (res.size() < numPop) {
                    // We got less than we requested, which means we emptied
                    // the list.
                    break;
                }
                cnt -= numPop;
            } while(cnt > 0);

            return !allRes.isEmpty() ?
                new ArrayRedisMessage(allRes) :
                FullBulkStringRedisMessage.NULL_INSTANCE;
        }
    }

    private RedisMessage handleLRPop(RedisClientContext client,
        RawCommand cmd, boolean isLeft) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);

        int cnt = -1;
        if (cmd.args.length == 2) {
            cnt = (int)Utils.byteBufToLong(cmd.args[1]);
            if (cnt < 0) {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "value out of range, must be positive");
            }
        }

        return handleLRPop(client, cmd.args[0], cnt, isLeft);
    }

    private RedisMessage doNonBlockingPop(RedisClientContext client,
        ByteBuf[] keys, int keyOff, int keyCnt, int cnt,
        boolean isLeft) throws RedisResponseException {
        // TODO: we can optimize this to query elements of multiple lists in a
        // single query using IN operator with an array of list keys. Same
        // applies to BLPOP, BRPOP and BLMPOP.

        for(int i = 0; i < keyCnt; i++) {
            ByteBuf keyBuf = keys[i + keyOff];
            RedisMessage res = handleLRPop(client, keyBuf, (int)cnt, isLeft);
            if (res != FullBulkStringRedisMessage.NULL_INSTANCE) {
                assert res instanceof ArrayRedisMessage;
                return new ArrayRedisMessage(Arrays.asList(
                    new FullBulkStringRedisMessage(keyBuf.retain()), res));
            }
        }

        return FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    // We do exponential backoff until delay reaches MAX_DELAY_MS, then we
    // retry with constant delay until timeout expires.
    private RedisMessage doBlockingPop(RedisClientContext client,
        ByteBuf[] keys, int keyOff, int keyCnt, double timeout, int cnt,
        boolean isLeft) throws RedisResponseException {
        long timeoutMs = (long)Math.ceil(timeout * 1000);
        long delay = MIN_DELAY_MS +
            (int)(Math.random() * MAX_ADD_RND_DELAY_MS);

        long expTime = timeout != 0 ?
            System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;

        while(true) {
            RedisMessage res = doNonBlockingPop(client, keys, keyOff, keyCnt,
                cnt, isLeft);
            if (res != FullBulkStringRedisMessage.NULL_INSTANCE) {
                return res;
            }

            long remaining = expTime - System.currentTimeMillis();
            if (remaining < MIN_DELAY_MS) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }

            if (delay < MAX_DELAY_MS) {
                delay = Math.min(MAX_DELAY_MS, delay * 2 +
                    (int)(Math.random() * MAX_ADD_RND_DELAY_MS));
            }

            delay = Math.min(delay, remaining);

            try {
                Thread.sleep(delay);
            } catch(InterruptedException ex) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
        }
    }

    private RedisMessage handleBLRPop(RedisClientContext client,
        RawCommand cmd, boolean isLeft) throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        int keyCnt = cmd.args.length - 1;
        double timeout = Utils.byteBufToDouble(cmd.args[cmd.args.length - 1]);
        return doBlockingPop(client, cmd.args, 0, keyCnt, timeout, 1,
            isLeft);
    }

    private RedisMessage handleLMPop(RedisClientContext client,
        RawCommand cmd, boolean isBlocking) throws RedisResponseException {
        int nkOff = isBlocking ? 1 : 0;
        chkMinNumArgs(cmd, 3 + nkOff);

        double timeout = 0;
        if (isBlocking) {
            timeout = Utils.byteBufToDouble(cmd.args[0]);
            if (timeout < 0) {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "timeout is negative");
            }
        }

        long numKeys = Utils.byteBufToLong(cmd.args[nkOff]);
        if (numKeys <= 0) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "numkeys should be greater than 0");
        }

        // Total count also includes timout (for blocking), numkeys and
        // LEFT/RIGHT. Incidentally, this will also check that
        // numKeys <= Integer.MAX_VALUE.
        if (cmd.args.length < numKeys + nkOff + 2) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        String leftRight = Utils.byteBufToString(
            cmd.args[(int)numKeys + nkOff + 1]);
        boolean isLeft = leftRight.equalsIgnoreCase(LEFT);
        if (!isLeft && !leftRight.equalsIgnoreCase(RIGHT)) {
            throw RedisResponseException.syntaxError();
        }

        long cnt = 1;
        // The only args following LEFT/RIGHT can be: COUNT <count_value>.
        if (cmd.args.length > numKeys + nkOff + 2) {
            if (cmd.args.length != numKeys + nkOff + 4 ||
                !Utils.byteBufToString(cmd.args[(int)numKeys + nkOff + 2])
                    .equalsIgnoreCase(COUNT)) {
                throw RedisResponseException.syntaxError();
            }
            cnt = Utils.byteBufToLong(cmd.args[(int)numKeys + nkOff + 3]);
            if (cnt <= 0) {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "count should be greater than 0");
            }
    }

        return isBlocking ?
            doBlockingPop(client, cmd.args, 2, (int)numKeys, timeout,
                (int)cnt, isLeft) :
            doNonBlockingPop(client, cmd.args, 1, (int)numKeys, (int)cnt,
                isLeft);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_LPUSH, this::handleLPush);
        cmdMap.put(CMD_RPUSH, this::handleRPush);
        cmdMap.put(CMD_LPUSHX, this::handleLPushX);
        cmdMap.put(CMD_RPUSHX, this::handleRPushX);
        cmdMap.put(CMD_LPOP, this::handleLPop);
        cmdMap.put(CMD_RPOP, this::handleRPop);
        cmdMap.put(CMD_LMPOP, this::handleLMPop);
        cmdMap.put(CMD_BLPOP, this::handleBLPop);
        cmdMap.put(CMD_BRPOP, this::handleBRPop);
        cmdMap.put(CMD_BLMPOP, this::handleBLMPop);
    }

    public RedisMessage handleLPush(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLRPush(client, cmd, true, false);
    }

    public RedisMessage handleRPush(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLRPush(client, cmd, false, false);
    }

    public RedisMessage handleLPushX(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLRPush(client, cmd, true, true);
    }

    public RedisMessage handleRPushX(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLRPush(client, cmd, false, true);
    }

    public RedisMessage handleLPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
             return handleLRPop(client, cmd, true);
    }

    public RedisMessage handleRPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
             return handleLRPop(client, cmd, false);
    }

    public RedisMessage handleLMPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLMPop(client, cmd, false);
    }

    public RedisMessage handleBLPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleBLRPop(client, cmd, true);
    }

    public RedisMessage handleBRPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleBLRPop(client, cmd, false);
    }

    public RedisMessage handleBLMPop(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLMPop(client, cmd, true);
    }

}
