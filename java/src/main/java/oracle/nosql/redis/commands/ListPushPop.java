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
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
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
import oracle.nosql.redis.util.Utils.ThrowingNoArgFunction;

public class ListPushPop extends ListCommandsBase {

    private static final String LEFT = "LEFT";
    private static final String RIGHT = "RIGHT";
    private static final String COUNT = "COUNT";

    private static final int MIN_DELAY_MS = 200;
    private static final int MAX_ADD_RND_DELAY_MS = 50;
    private static final int MAX_DELAY_MS = 5000;

    private static final String SQL_LMOVE_LEFT = String.format(SQL_ELEMS_FMT,
        "", ELEM_VAL, "", PK_COLS, LIMIT_1, "");
    private static final String SQL_LMOVE_RIGHT = String.format(SQL_ELEMS_FMT,
        "", ELEM_VAL, "", PK_COLS_DESC, LIMIT_1, "");

    public ListPushPop(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    private static boolean getIsLeft(ByteBuf buf)
        throws RedisResponseException {
        String leftRight = Utils.byteBufToString(buf);
        if (leftRight.equalsIgnoreCase(LEFT)) {
            return true;
        };

        if (!leftRight.equalsIgnoreCase(RIGHT)) {
            throw RedisResponseException.syntaxError();
        }

        return false;
    }

    // Next new id for push.
    private static BigDecimal getNextNewId(BigDecimal id, boolean isLeft) {
        return isLeft ?
            id.setScale(0, RoundingMode.HALF_DOWN).subtract(BigDecimal.ONE) :
            id.setScale(0, RoundingMode.HALF_UP).add(BigDecimal.ONE);
    }

    private static ListHeader addPushListElems(RedisKeyInfo keyInfo,
        ListValueInfo lvi, ByteBuf []  elems, int off, int cnt, boolean isLeft,
        WriteMultipleRequest wmReq, RedisValueInfo oldVal)
        throws RedisResponseException {
        ListHeader header = lvi != null ? lvi.header : null;
        BigDecimal startId;
        if (header != null) {
            assert !lvi.elemIds.isEmpty();
            chkSingleResult(lvi.elemIds);
            startId = getNextNewId(lvi.elemIds.get(0), isLeft);
            header.len += cnt;
        } else { // The list doesn't exist or expired.
            startId = BigDecimal.ZERO;
            // Create new list with cnt elements.
            header = new ListHeader(cnt);
        }

        wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);

        int end = off + cnt;
        for(int i = off; i < end; i++) {
            wmReq.add(makePutElemReq(keyInfo, startId, header.cid, elems[i]),
                false);
            startId = isLeft ? startId.subtract(BigDecimal.ONE) :
                startId.add(BigDecimal.ONE);
        }

        return header;
    }

    private ListHeader doLRPush(RedisKeyInfo keyInfo, ByteBuf[] elems,
        int off, int cnt, boolean isLeft, boolean ifExists)
        throws RedisResponseException {
        assert cnt > 0;
        return doWithRetries(() -> {
            CollectionValueResult<ListValueInfo> cvr =
                queryListElems(keyInfo, isLeft ? SQL_LPUSH : SQL_RPUSH);
            if (ifExists && !cvr.isValid()) {
                return null;
            }
            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            ListHeader header = addPushListElems(keyInfo, cvr.data, elems, off,
                cnt, isLeft, wmReq, cvr.val);
            doWM(wmReq, true);
            return header;
        });
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

        return doWithRetries(() -> {
            CollectionValueResult<ListValueInfo> cvr = queryListElems(keyInfo,
                isLeft ? SQL_LPOP : SQL_RPOP, new LongValue(cnt));
            if (!cvr.isValid()) {
                return null;
            }

            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            addDeleteListElems(keyInfo, cvr.data, wmReq, cvr.val, true);
            WriteMultipleResult wmRes = doWM(wmReq, true);

            List<OperationResult> opsRes = wmRes.getResults();
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
                        ERR_MISSING_EXISTING_VAL);
                }
                vals.add(new FullBulkStringRedisMessage(
                    getStrVal(Utils.getStringField(val, FLD_VALUE))));
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
                    ArrayRedisMessage.NULL_INSTANCE :
                    ArrayRedisMessage.EMPTY_INSTANCE;
            }

            List<RedisMessage> res = doLRPop(keyInfo, cnt == -1 ? 1 : cnt,
                isLeft);
            if (res == null) {
                // Mimics the observed behavior that array null instance is
                // returned if count is specified.
                return cnt == -1 ?
                    FullBulkStringRedisMessage.NULL_INSTANCE :
                    ArrayRedisMessage.NULL_INSTANCE;
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
                ArrayRedisMessage.NULL_INSTANCE;
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

    private RedisMessage doMultiKeyPop(RedisClientContext client,
        ByteBuf[] keys, int keyOff, int keyCnt, int cnt,
        boolean isLeft) throws RedisResponseException {
        // TODO: we can optimize this to query elements of multiple lists in a
        // single query using IN operator with an array of list keys. Same
        // applies to BLPOP, BRPOP and BLMPOP.

        for(int i = 0; i < keyCnt; i++) {
            ByteBuf keyBuf = keys[i + keyOff];
            RedisMessage res = handleLRPop(client, keyBuf, cnt, isLeft);
            if (res != FullBulkStringRedisMessage.NULL_INSTANCE &&
                res != ArrayRedisMessage.NULL_INSTANCE) {
                assert res instanceof ArrayRedisMessage;
                return new ArrayRedisMessage(Arrays.asList(
                    new FullBulkStringRedisMessage(keyBuf.retain()), res));
            }
        }

        return FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    private RedisMessage doLMove(RedisKeyInfo srcKeyInfo,
        RedisKeyInfo dstKeyInfo, boolean isSrcLeft, boolean isDstLeft)
        throws RedisResponseException {
        if (srcKeyInfo.slot != dstKeyInfo.slot) {
            throw RedisResponseException.crossSlot();
        }

        CollectionValueResult<ListValueInfo> srcValInfo = queryListElems(
            srcKeyInfo, isSrcLeft ? SQL_LMOVE_LEFT : SQL_LMOVE_RIGHT, false,
            true);
        if (!srcValInfo.val.isValid()) {
            // Source list doesn't exist or expired.
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        chkSingleResult(srcValInfo.data.elemIds);
        ByteBuf retVal = getStrVal(srcValInfo.data.elemVals.get(0));

        boolean isSameKey = srcKeyInfo.id.equals(dstKeyInfo.id);
        if (isSameKey && isSrcLeft == isDstLeft) {
            return new FullBulkStringRedisMessage(retVal);
        }

        CollectionValueResult<ListValueInfo> dstValInfo = queryListElems(
            dstKeyInfo, isDstLeft ? SQL_LPUSH : SQL_RPUSH);

        WriteMultipleRequest wmReq = new WriteMultipleRequest();

        if (isSameKey) {
            // Special case when src and dst keys are the same and the element
            // is rotated within the same list. In this case we don't update
            // the parent row.
            wmReq.add(makeDeleteElemReq(srcKeyInfo,
                srcValInfo.data.elemIds.get(0), false), false);
            wmReq.add(makePutElemReq(dstKeyInfo,
                getNextNewId(dstValInfo.data.elemIds.get(0), isDstLeft),
                dstValInfo.data.header.cid, retVal), false);
        } else {
            addDeleteListElems(srcKeyInfo, srcValInfo.data, wmReq,
                srcValInfo.val, false);
            addPushListElems(dstKeyInfo, dstValInfo.data,
                new ByteBuf[]{retVal}, 0, 1, isDstLeft, wmReq, dstValInfo.val);
        }

        doWM(wmReq, true);
        return new FullBulkStringRedisMessage(retVal);
    }

    // We do exponential backoff until delay reaches MAX_DELAY_MS, then we
    // retry with constant delay until timeout expires.
    private RedisMessage doBlockingOp(
        RedisClientContext client,
        ThrowingNoArgFunction<RedisMessage, RedisResponseException> op,
        double timeout) throws RedisResponseException {
        if (timeout < 0) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "timeout is negative");
        }

        long delay = MIN_DELAY_MS +
            (int)(Math.random() * MAX_ADD_RND_DELAY_MS);

        long expTime = timeout != 0 ?
            System.currentTimeMillis() + (long)Math.ceil(timeout * 1000) :
            Long.MAX_VALUE;

        client.setBlocked(true);
        try {
            while (true) {
                RedisMessage res = doWithRetries(op::apply);
                if (res != FullBulkStringRedisMessage.NULL_INSTANCE) {
                    return res;
                }

                long remaining = expTime - System.currentTimeMillis();
                if (remaining < MIN_DELAY_MS) {
                    return FullBulkStringRedisMessage.NULL_INSTANCE;
                }

                if (delay < MAX_DELAY_MS) {
                    delay = Math.min(MAX_DELAY_MS, delay * 2 +
                        (int) (Math.random() * MAX_ADD_RND_DELAY_MS));
                }

                delay = Math.min(delay, remaining);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ex) {
                    return FullBulkStringRedisMessage.NULL_INSTANCE;
                }
            }
        } finally {
            client.setBlocked(false);
        }
    }

    private RedisMessage handleBLRPop(RedisClientContext client,
        RawCommand cmd, boolean isLeft) throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        int keyCnt = cmd.args.length - 1;
        double timeout = Utils.byteBufToDouble(cmd.args[cmd.args.length - 1]);
        return doBlockingOp(client,
            () -> doMultiKeyPop(client, cmd.args, 0, keyCnt, 1, isLeft),
            timeout);
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

        boolean isLeft = getIsLeft(cmd.args[(int)numKeys + nkOff + 1]);

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

        final int elemCnt = (int)cnt;
        return isBlocking ?
            doBlockingOp(client, () -> doMultiKeyPop(client, cmd.args, 2,
                (int)numKeys, elemCnt, isLeft), timeout) :
            doMultiKeyPop(client, cmd.args, 1, (int)numKeys, (int)cnt,
                isLeft);
    }

    private RedisMessage handleLMove(RedisClientContext client,
        ByteBuf srcKeyBuf, ByteBuf dstKeyBuf, boolean isSrcLeft,
        boolean isDstLeft, ByteBuf timeoutBuf) throws RedisResponseException {
        RedisKeyInfo srcKeyInfo = makeRedisKeyInfo(srcKeyBuf);
        RedisKeyInfo dstKeyInfo = makeRedisKeyInfo(dstKeyBuf);

        if (timeoutBuf != null) {
            double timeout = Utils.byteBufToDouble(timeoutBuf);
            return doBlockingOp(client, () -> doLMove(srcKeyInfo, dstKeyInfo,
                isSrcLeft, isDstLeft), timeout);
        }

        return doWithRetries(
            () -> doLMove(srcKeyInfo, dstKeyInfo, isSrcLeft, isDstLeft));
    }

    private RedisMessage handleLMove(RedisClientContext client, RawCommand cmd,
        boolean isBlocking) throws RedisResponseException {
        chkExactNumArgs(cmd, isBlocking ? 5 : 4);
        return handleLMove(client, cmd.args[0], cmd.args[1],
            getIsLeft(cmd.args[2]), getIsLeft(cmd.args[3]),
            isBlocking ? cmd.args[4] : null);
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
        cmdMap.put(CMD_LMOVE, this::handleLMove);
        cmdMap.put(CMD_RPOPLPUSH, this::handleRPopLPush);
        cmdMap.put(CMD_BLMOVE, this::handleBLMove);
        cmdMap.put(CMD_BRPOPLPUSH, this::handleBRPopLPush);
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

    public RedisMessage handleLMove(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLMove(client, cmd, false);
    }

    public RedisMessage handleRPopLPush(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        return handleLMove(client, cmd.args[0], cmd.args[1], false, true,
            null);
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

    public RedisMessage handleBLMove(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return handleLMove(client, cmd, true);
    }

    public RedisMessage handleBRPopLPush(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        return handleLMove(client, cmd.args[0], cmd.args[1], false, true,
            cmd.args[2]);
    }

}
