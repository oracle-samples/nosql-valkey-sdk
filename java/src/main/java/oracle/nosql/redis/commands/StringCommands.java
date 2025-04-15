/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.NoSQLRedisServer;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.RedisRetryException;
import oracle.nosql.redis.util.Utils.ThrowingFunction;

public class StringCommands extends CommandsBase {

    private static final String AND_IS_STRING =
        "AND $r.value.type = '" + TYPE_STRING + "'";
    private static final String SQL_GET_DEL = DECL_KEY_ID +
        "DELETE FROM redis $r WHERE " + KEY_ID_COND + AND_NOT_EXPIRED +
        AND_IS_STRING + SQL_RETURNING + "$r.value.data AS value";
    private static final String SQL_MGET = DECL_KEY_IDS +
        "SELECT $r.id, $r.value.data AS data FROM redis $r " +
        WHERE_KEY_IDS_COND + AND_NOT_EXPIRED + AND_IS_STRING;
    // This query is used for MSETNX to check if there are existing keys. If
    // there are any existing non-expired keys, MSETNX will not proceed. Note
    // that the key may be expired but still exist in the table. We retrieve
    // these keys and get their versions, so that their update can be version-
    // conditioned in order to make MSETNX operation atomic (for non-existing
    // keys will will use if-absent instead).
    private static final String SQL_MSET_GET = DECL_KEY_IDS +
        "SELECT $r.id, row_version($r) AS ver, $r.key.exp  AS exp FROM " +
        "redis $r " + WHERE_KEY_IDS_COND;

    public static final String CMD_GET = "GET";
    public static final String CMD_GETRANGE = "GETRANGE";
    public static final String CMD_SUBSTR = "SUBSTR";
    public static final String CMD_STRLEN = "STRLEN";
    public static final String CMD_SET = "SET";
    public static final String CMD_SETNX = "SETNX";
    public static final String CMD_SETEX = "SETEX";
    public static final String CMD_PSETEX = "PSETEX";
    public static final String CMD_APPEND = "APPEND";
    public static final String CMD_SETRANGE = "SETRANGE";
    public static final String CMD_INCR = "INCR";
    public static final String CMD_INCRBY = "INCRBY";
    public static final String CMD_INCRBYFLOAT = "INCRBYFLOAT";
    public static final String CMD_DECR = "DECR";
    public static final String CMD_DECRBY = "DECRBY";
    public static final String CMD_MGET = "MGET";
    public static final String CMD_MSET = "MSET";
    public static final String CMD_MSETNX = "MSETNX";
    public static final String CMD_GETDEL = "GETDEL";
    public static final String CMD_GETSET = "GETSET";

    // Max row size in the cloud is 512KB, however the row will store more
    // than just value (in particular, key).  We can adjust this later.
    static final int MAX_STR_LEN = 256 * 1024;

    public StringCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private static ByteBuf getStringValue(MapValue val)
        throws RedisResponseException {
        if (val == null) {
            return null;
        }

        if (!getValueType(val).equals(TYPE_STRING)) {
            throw RedisResponseException.wrongType();
        }

        return getStrVal(getData(val));
    }

    private static MapValue makeStringValue(ByteBuf buf) {
        return new MapValue().put(VALUE_TYPE, TYPE_STRING)
            .put(VALUE_DATA, makeStrVal(buf));
    }

    private ByteBuf valInfoToByteBuf(RedisValueInfo valInfo)
        throws RedisResponseException {
        return valInfo.isValid() ? getStringValue(valInfo.val) : null;
    }

    private RedisMessage byteBufToResp(ByteBuf buf) {
        return buf != null ? new FullBulkStringRedisMessage(buf) :
            FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    private RedisMessage valInfoToResp(RedisValueInfo valInfo)
        throws RedisResponseException {
        return byteBufToResp(valInfoToByteBuf(valInfo));
    }

    private ByteBuf doGetValue(RedisKeyInfo keyInfo)
        throws RedisResponseException {
        RedisValueInfo vi = doGet(keyInfo);
        return valInfoToByteBuf(vi);
    }

    private ByteBuf doGetValue(ByteBuf keyBuf)
        throws RedisResponseException {
        return doGetValue(makeRedisKeyInfo(keyBuf));
    }

    private RedisMessage doGetString(ByteBuf keyBuf)
        throws RedisResponseException {
        ByteBuf res = doGetValue(keyBuf);
        return byteBufToResp(res);
    }

    private RedisMessage doSetString(ByteBuf keyBuf, long exp, ByteBuf valBuf,
        SetOpt setOpt, boolean toGetOldVal, RedisMessage successReply,
        RedisMessage failureReply)
        throws RedisResponseException {
        // If the result does not depend on the old value, we don't need
        // version-conditioned put or retries done by doGetSet().
        if (!toGetOldVal && exp != KEEP_TTL) {
            return doSet(keyBuf, makeStringValue(valBuf), exp, setOpt) ?
                successReply : failureReply;
        }
        return doGetSet(
            keyBuf,
            (oldVal) -> setOpt == null ||
                (setOpt == SetOpt.NX && !oldVal.exists()) ||
                (setOpt == SetOpt.XX && oldVal.exists()),
            (oldVal) -> {
                if (toGetOldVal && oldVal.val != null &&
                    !getValueType(oldVal.val).equals(TYPE_STRING)) {
                    throw RedisResponseException.wrongType();
                }
                return new RedisValueInfo(makeStringValue(valBuf), null,
                    exp != KEEP_TTL ? exp : oldVal.exp);
            },
            // newVal is NONE if SET is not successful
            (oldVal, newVal) -> {
                assert(setOpt != null || newVal.exists());
                return toGetOldVal ? valInfoToResp(oldVal) :
                    (newVal.exists() ? successReply : failureReply);
            }, true);
    }

    private RedisMessage doSetString(ByteBuf keyBuf, long exp, ByteBuf valBuf,
        SetOpt setOpt, boolean toGetOldVal)
        throws RedisResponseException {
        return doSetString(keyBuf, exp, valBuf, setOpt, toGetOldVal, okReply,
            FullBulkStringRedisMessage.NULL_INSTANCE);
    }

    // For SETEX, PSETEX.
    private RedisMessage doSetWithExp(RawCommand cmd, TTLMode ttlMode)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        long exp = makeExpTime(cmd.args[1], ttlMode);
        return doSetString(cmd.args[0], exp, cmd.args[2], null, false);
    }

    private RedisMessage doGetSetString(ByteBuf keyBuf,
        ThrowingFunction<ByteBuf,ByteBuf,RedisResponseException> getNewVal,
        ThrowingFunction<ByteBuf,RedisMessage,RedisResponseException>
            getResult) throws RedisResponseException {
        return doGetSet(keyBuf, (oldVal) -> true,
            (oldVal) -> new RedisValueInfo(makeStringValue(
                getNewVal.apply(getStringValue(oldVal.val))), null, NO_EXP),
            (oldVal, newVal) -> getResult.apply(getStringValue(newVal.val)),
            true);
    }

    private RedisMessage doIncrBy(ByteBuf keyBuf, final long arg)
        throws RedisResponseException {
        // It is in principle possible to avoid repeating the conversion
        // between long and ByteBuf in getResult by making getNewVal,
        // doGetSEtString and doGetSet generic and providing additional
        // conversion interface args, however this will add more complexity to
        // doGetSet, so may not be worth it.
        return doGetSetString(keyBuf,
            (val) -> Utils.longToByteBuf((val != null ?
                Utils.byteBufToLong(val) : 0) + arg),
                val -> new IntegerRedisMessage(Utils.byteBufToLong(val)));
    }

    private RedisMessage handleMSet(RedisClientContext client, RawCommand cmd,
        boolean isNX) throws RedisResponseException {
        if (cmd.args == null || cmd.args.length < 2 ||
            cmd.args.length % 2 == 1) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        if (cmd.args.length > 2 * MAX_WM_CNT) {
            throw new RedisResponseException(ErrorPrefix.ERR, String.format(
                "Atomic update of more than %s keys is not supported",
                MAX_WM_CNT));
        }

        RedisKeyInfo[] keyInfos = makeRedisMultiKeyInfo(cmd.args, 0,
            cmd.args.length / 2, 2);

        HashMap<String, oracle.nosql.driver.Version> expVers = null;
        if (isNX) {
            ArrayValue keyIds = makeKeyIdsValue(keyInfos);
            PreparedStatement pStmt = pstmtCache.getByRef(SQL_MSET_GET);
            pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfos[0].slot));
            pStmt.setVariable(SQL_KEY_IDS, keyIds);
    
            final HashMap<String, oracle.nosql.driver.Version> hs =
                new HashMap<>();
            if (processQuery(pStmt, row -> {
                // we are retrieving "exp" field so we use the key function
                if (!isKeyExpired(row)) {
                    return true;
                }
                hs.put(getId(row), rowToVer(row));
                return false;
            })) {
                // processQuery will return true if it encounters an unexpired
                // key
                return zeroReply;
            };

            expVers = hs;
        }

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        for(int i = 0; i < keyInfos.length; i++) {
            RedisKeyInfo ki = keyInfos[i];
            MapValue row = new MapValue().put(FLD_SLOT, ki.slot)
                .put(FLD_ID, ki.id).put(FLD_KEY, makeRedisKey(ki))
                // cmd.args alternate keys and values
                .put(FLD_VALUE, makeStringValue(cmd.args[i * 2 + 1]));
            PutRequest putReq = new PutRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setValue(row);
            if (isNX) {
                oracle.nosql.driver.Version expVer = expVers.get(ki.id);
                if (expVer == null) {
                    putReq.setOption(PutRequest.Option.IfAbsent);
                } else {
                    putReq.setOption(PutRequest.Option.IfVersion);
                    putReq.setMatchVersion(expVer);                
                }
            }
            wmReq.add(putReq, true);
        }
        
        try {
            WriteMultipleResult res = nosqlHandle.writeMultiple(wmReq);
            if (!res.getSuccess()) {
                if (isNX) {
                    throw new RedisRetryException();
                }
                throw RedisResponseException.nosql(ERR_WM_FAIL);
            }
            return isNX ? oneReply : okReply;
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_GET, this::handleGet);
        cmdMap.put(CMD_MGET, this::handleMGet);
        cmdMap.put(CMD_GETRANGE, this::handleGetRange);
        // SUBSTR deprecated, replaced by GETRANGE
        cmdMap.put(CMD_SUBSTR, this::handleGetRange);
        cmdMap.put(CMD_STRLEN, this::handleStrlen);
        cmdMap.put(CMD_SET, this::handleSet);
        cmdMap.put(CMD_SETNX, this::handleSetNX);
        cmdMap.put(CMD_SETEX, this::handleSetEX);
        cmdMap.put(CMD_PSETEX, this::handlePSetEX);
        cmdMap.put(CMD_APPEND, this::handleAppend);
        cmdMap.put(CMD_SETRANGE, this::handleSetRange);
        cmdMap.put(CMD_INCR, this::handleIncr);
        cmdMap.put(CMD_INCRBY, this::handleIncrBy);
        cmdMap.put(CMD_INCRBYFLOAT, this::handleIncrByFloat);
        cmdMap.put(CMD_DECR, this::handleDecr);
        cmdMap.put(CMD_DECRBY, this::handleDecrBy);
        cmdMap.put(CMD_MSET, this::handleMSet);
        cmdMap.put(CMD_MSETNX, this::handleMSetNX);
        cmdMap.put(CMD_GETDEL, this::handleGetDel);
        cmdMap.put(CMD_GETSET, this::handleGetSet);
    }

    public RedisMessage handleGet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doGetString(cmd.args[0]);
    }

    public RedisMessage handleMGet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
   
        RedisKeyInfo[] keyInfos = makeRedisMultiKeyInfo(cmd.args, 0,
            cmd.args.length);
        ArrayValue keyIds = makeKeyIdsValue(keyInfos);

        PreparedStatement pStmt = pstmtCache.getByRef(SQL_MGET);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfos[0].slot));
        pStmt.setVariable(SQL_KEY_IDS, keyIds);

        // We need to return results for all provided keys in order, but the
        // query results may be in different order and/or missing non-existent
        // keys or keys of wrong type, so we need to collect query results
        // first to create the final result.
        HashMap<String,String> resMap = new HashMap<>();
        processQuery(pStmt, row ->  {
            resMap.put(getId(row), getData(row));
        });

        ArrayList<RedisMessage> res = new ArrayList<>();
        for(FieldValue keyId: keyIds) {
            String data = resMap.get(keyId.getString());
            res.add(data != null ?
                new FullBulkStringRedisMessage(getStrVal(data)) :
                FullBulkStringRedisMessage.NULL_INSTANCE);
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleGetRange(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        ByteBuf buf = doGetValue(cmd.args[0]);
        // if key not found, return empty string
        if (buf == null) {
            return FullBulkStringRedisMessage.EMPTY_INSTANCE;
        }
        
        long start = Utils.byteBufToLong(cmd.args[1]);
        long end = Utils.byteBufToLong(cmd.args[2]);
        int len = buf.readableBytes();
        
        if (start < 0) {
            start = Math.max(len + start, 0);
        }
        if (end < 0) {
            end = Math.max(len + end, 0);
        }
        if (end > len) {
            end = len;
        }

        // Note that for this command both start and end are inclusive.
        return end >= start ? new FullBulkStringRedisMessage(
            buf.slice((int)start, (int)(end - start + 1))) :
            FullBulkStringRedisMessage.EMPTY_INSTANCE;
    }

    public RedisMessage handleStrlen(RedisClientContext client,
    RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        ByteBuf buf = doGetValue(cmd.args[0]);
        return buf != null ? new IntegerRedisMessage(buf.readableBytes()) :
            zeroReply;
    }

    public RedisMessage handleSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        // The first 2 args must be key and value.
        chkMinNumArgs(cmd, 2);
        SetOpt setOpt = null;
        TTLMode ttlMode = null;
        boolean isGet = false;
        ByteBuf expArg = null;

        for(int i = 2; i < cmd.args.length; i++) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("NX")) {
                chkNotSet(setOpt);
                setOpt = SetOpt.NX;
            } else if (arg.equalsIgnoreCase("XX")) {
                chkNotSet(setOpt);
                setOpt = SetOpt.XX;
            } else if (arg.equalsIgnoreCase("GET")) {
                isGet = true;
            } else if (arg.equalsIgnoreCase("KEEPTTL")) {
                chkNotSet(ttlMode);
                ttlMode = TTLMode.KEEP_TTL;
            } else {
                if (++i == cmd.args.length) {
                    throw RedisResponseException.syntaxError();
                }
                expArg = cmd.args[i];
                if (arg.equalsIgnoreCase("EX")) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.EX;
                } else if (arg.equalsIgnoreCase("PX")) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.PX;
                } else if (arg.equalsIgnoreCase("EXAT")) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.EXAT;
                } else if (arg.equalsIgnoreCase("PXAT")) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.PXAT;
                } else {
                    throw RedisResponseException.syntaxError();
                }
            }
        }
        
        return doSetString(cmd.args[0], makeExpTime(expArg, ttlMode),
            cmd.args[1], setOpt, isGet);
    }

    public RedisMessage handleSetNX(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        return doSetString(cmd.args[0], NO_EXP, cmd.args[1], SetOpt.NX, false,
            oneReply, zeroReply);
    }

    public RedisMessage handleSetEX(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return doSetWithExp(cmd, TTLMode.EX);
    }

    public RedisMessage handlePSetEX(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return doSetWithExp(cmd, TTLMode.PX);
    }

    public RedisMessage handleAppend(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final ByteBuf arg = cmd.args[1];
        return doGetSetString(cmd.args[0],
            (val) -> val != null ? Unpooled.wrappedBuffer(val, arg) : arg,
            (val) -> new IntegerRedisMessage(val.readableBytes()));
    }

    public RedisMessage handleSetRange(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        
        final long off = Utils.byteBufToLong(cmd.args[1]);
        if (off < 0) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "offset is out of range");
        }
        
        final ByteBuf rangeVal = cmd.args[2];
        
        return doGetSetString(cmd.args[0],
            (val) -> {
                int newLen = (int)Math.max(off + rangeVal.readableBytes(),
                    val.readableBytes());
                if (newLen > MAX_STR_LEN) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "string exceeds maximum allowed size");
                }
                if (val.capacity() < newLen) {
                    val.capacity(newLen);
                }
                return val.setBytes((int)off, rangeVal, 0,
                    rangeVal.readableBytes()).writerIndex(newLen);
            },
            (val) -> new IntegerRedisMessage(val.readableBytes()));
    }

    public RedisMessage handleIncr(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doIncrBy(cmd.args[0], 1);
    }

    public RedisMessage handleIncrBy(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final long arg = Utils.byteBufToLong(cmd.args[1]);
        return doIncrBy(cmd.args[0], arg);
    }

    public RedisMessage handleDecr(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doIncrBy(cmd.args[0], -1);
    }

    public RedisMessage handleDecrBy(RedisClientContext client,
    RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final long arg = Utils.byteBufToLong(cmd.args[1]);
        return doIncrBy(cmd.args[0], -arg);
    }

    public RedisMessage handleIncrByFloat(RedisClientContext client,
    RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final double arg = Utils.byteBufToDouble(cmd.args[1]);
        return doGetSetString(cmd.args[0],
            (val) -> Utils.doubleToByteBuf(
                (val != null ? Utils.byteBufToDouble(val) : 0) + arg),
            (val) -> new FullBulkStringRedisMessage(val));
    }

    public RedisMessage handleGetSet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        return doSetString(cmd.args[0], NO_EXP, cmd.args[1], null, true);
    }

    public RedisMessage handleGetDel(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        PreparedStatement pStmt = pstmtCache.getByRef(SQL_GET_DEL);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));

        final String [] val = new String[1];
        processQuery(pStmt, row -> {
            if (val[0] != null) {
                throw RedisResponseException.nosql(ERR_NO_SINGLE_RES);
            }
            val[0] = getStringField(row, FLD_VALUE);
        });

        return val != null ?
            new FullBulkStringRedisMessage(getStrVal(val[0])) :
            FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleMSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return handleMSet(client, cmd, false);
    }

    public RedisMessage handleMSetNX(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return Utils.doWithRetries(() -> handleMSet(client, cmd, true),
            ATOMIC_SET_TRIES);
    }

}
