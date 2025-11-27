/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.*;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.RedisRetryException;
import oracle.nosql.redis.util.Utils.ThrowingFunction;

import static oracle.nosql.redis.util.Utils.getStringField;

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
    // keys will use if-absent instead).
    private static final String SQL_MSET_GET = DECL_KEY_IDS +
        "SELECT $r.id, row_version($r) AS ver, $r.key.exp  AS exp FROM " +
        "redis $r " + WHERE_KEY_IDS_COND;

    private static final String ERR_MAX_SIZE =
        "string exceeds maximum allowed size";

    // Max row size in the cloud is 512KB, however the row will store more
    // than just value (in particular, key).  We can adjust this later.
    protected static final int MAX_STR_LEN = 256 * 1024;

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
    public static final String CMD_GETEX = "GETEX";

    public StringCommands(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
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

    private static MapValue makeStringValue(ByteBuf buf)
        throws RedisResponseException {
        if (buf.readableBytes() > MAX_STR_LEN) {
            throw new RedisResponseException(ErrorPrefix.ERR, ERR_MAX_SIZE);
        }
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
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);

        // If the result does not depend on the old value, we don't need
        // version-conditioned put or retries.
        // Note that doSet() cannot handle SetOpt.XX.
        // Todo: for SetOpt.XX, use update query instead of put.
        if (!toGetOldVal && exp != KEEP_TTL && setOpt != SetOpt.XX) {
            return doSet(keyInfo, makeStringValue(valBuf), exp,
                setOpt == SetOpt.NX) ? successReply : failureReply;
        }

        return doWithRetries(() -> {
            RedisValueInfo oldVal = doGet(keyInfo);
            if (setOpt != null && (setOpt == SetOpt.NX && oldVal.isValid()) ||
                (setOpt == SetOpt.XX && !oldVal.isValid())) {
                return toGetOldVal ? valInfoToResp(oldVal) : failureReply;
            }
            if (toGetOldVal && oldVal.isValid() &&
                !getValueType(oldVal.val).equals(TYPE_STRING)) {
                throw RedisResponseException.wrongType();
            }
            RedisValueInfo newVal = RedisValueInfo.create(
                makeStringValue(valBuf), null, exp);
            doChkSet(keyInfo, newVal, oldVal);
            return toGetOldVal ? valInfoToResp(oldVal) : successReply;
        });
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
        long exp = makeExpTime(cmd.args[1], ttlMode, cmd.name, true);
        return doSetString(cmd.args[0], exp, cmd.args[2], null, false);
    }

    private RedisMessage doGetSetString(ByteBuf keyBuf,
        ThrowingFunction<ByteBuf,ByteBuf,RedisResponseException> getNewVal,
        ThrowingFunction<ByteBuf,RedisMessage,RedisResponseException>
            getResult) throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);
        return doWithRetries(() -> {
            RedisValueInfo oldVal = doGet(keyInfo);
            ByteBuf newBuf = getNewVal.apply(getStringValue(oldVal.val));
            // We use newBuf = null to indicate that update should not be
            // performed (used for one special case in handleSetRange).
            if (newBuf != null) {
                RedisValueInfo newVal = RedisValueInfo.create(
                    makeStringValue(newBuf), null, NO_EXP);
                doChkSet(keyInfo, newVal, oldVal);
            }
            // If getNewVal can return null, getResult must also handle null.
            return getResult.apply(newBuf);
        });
    }

    private RedisMessage doIncrBy(ByteBuf keyBuf, long arg, boolean toAdd)
        throws RedisResponseException {
        // It is in principle possible to avoid repeating the conversion
        // between long and ByteBuf in getResult by making getNewVal,
        // doGetSetString and doGetSet generic and providing additional
        // conversion interface args, however this will add more complexity to
        // doGetSet, so may not be worth it.
        return doGetSetString(keyBuf, val -> Utils.incrBy(val, arg, toAdd),
            val -> new IntegerRedisMessage(Utils.byteBufToLong(val)));
    }

    private RedisMessage handleMSet(RedisClientContext client, RawCommand cmd,
        boolean isNX) throws RedisResponseException {
        if (cmd.args == null || cmd.args.length < 2 ||
            cmd.args.length % 2 == 1) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        if (cmd.args.length > 2 * MAX_WM_CNT) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                ERR_TOO_MANY_KEYS);
        }

        RedisKeyInfo[] keyInfos = makeRedisMultiKeyInfo(cmd.args, 0,
            cmd.args.length / 2, 2);

        HashMap<String, oracle.nosql.driver.Version> expVers = null;
        if (isNX) {
            ArrayValue keyIds = makeKeyIdsValue(keyInfos);
            PreparedStatement pStmt = pstmtCache.getByRef(SQL_MSET_GET);
            pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfos[0].slot));
            pStmt.setVariable(SQL_KEY_IDS, keyIds);

            // Avoid allocating hashmap, since in most cases we won't need it.
            @SuppressWarnings("unchecked")
            final HashMap<String,oracle.nosql.driver.Version>[] hs =
                new HashMap[1];
            if (processQuery(pStmt, row -> {
                // we are retrieving "exp" field so we use the key function
                if (!isKeyExpired(row)) {
                    return true;
                }
                if (hs[0] == null) {
                    hs[0] = new HashMap<>();
                }
                hs[0].put(getId(row), rowToVer(row));
                return false;
            })) {
                // processQuery will return true if it encounters an unexpired
                // key
                return zeroReply;
            };

            expVers = hs[0];
        }

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        HashSet<String> chkDupSet = new HashSet<>();
        // The order of sub-requests in WriteMultipleRequest shouldn't matter.
        // However, we have to check for duplicate keys. The behavior on MSET
        // if duplicate keys are provided is to use the last provided value for
        // that key, hence our backward iteration.
        for(int i = keyInfos.length - 1; i >= 0; i--) {
            RedisKeyInfo ki = keyInfos[i];
            if (!chkDupSet.add(ki.id)) {
                continue;
            }
            MapValue row = new MapValue().put(FLD_SLOT, ki.slot)
                .put(FLD_ID, ki.id).put(FLD_KEY, makeRedisKey(ki))
                // cmd.args alternate keys and values
                .put(FLD_VALUE, makeStringValue(cmd.args[i * 2 + 1]));
            // Note that MSET removes any existing expiration time from a key.
            PutRequest putReq = new PutRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setValue(row).setTTL(TimeToLive.DO_NOT_EXPIRE);
            if (isNX) {
                oracle.nosql.driver.Version expVer =
                    expVers == null ? null : expVers.get(ki.id);
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
        cmdMap.put(CMD_GETEX, this::handleGetEx);
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

        // Supposedly, when indexes are too negative that they are out of
        // range (negative even after adding the value length), they are
        // converted to 0, so if this is the case for both start and end, the
        // first byte of the value will be returned. However, the observed
        // behavior is that when both indexes are negative and start > end,
        // empty string is returned. This does not make sense to me, but we
        // have to comply with the Redis behavior in this case.
        if (start < 0 && start > end) {
            return FullBulkStringRedisMessage.EMPTY_INSTANCE;
        }
        
        if (start < 0) {
            start = Math.max(len + start, 0);
        }
        if (end < 0) {
            end = Math.max(len + end, 0);
        }
        if (end >= len) {
            end = len - 1;
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
                if (arg.equalsIgnoreCase(TTLMode.EX.name())) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.EX;
                } else if (arg.equalsIgnoreCase(TTLMode.PX.name())) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.PX;
                } else if (arg.equalsIgnoreCase(TTLMode.EXAT.name())) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.EXAT;
                } else if (arg.equalsIgnoreCase(TTLMode.PXAT.name())) {
                    chkNotSet(ttlMode);
                    ttlMode = TTLMode.PXAT;
                } else {
                    throw RedisResponseException.syntaxError();
                }
            }
        }
        
        return doSetString(cmd.args[0],
            makeExpTime(expArg, ttlMode, cmd.name, true), cmd.args[1],
            setOpt, isGet);
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
        if (off > MAX_STR_LEN) {
            throw new RedisResponseException(ErrorPrefix.ERR, ERR_MAX_SIZE);
        }
        
        final ByteBuf rangeVal = cmd.args[2];
        int rangeValLen = rangeVal.readableBytes();
        
        return doGetSetString(cmd.args[0],
            (val) -> {
                if (val == null) {
                    // If the key does not exist and the provided value is
                    // empty string, Redis will not create the key. I did not
                    // see this behavior documented, but verified it, and it is
                    // also assumed this way in the unit tests.
                    if (rangeValLen == 0) {
                        return null;
                    }
                    val = Unpooled.buffer(0);
                }
                int oldLen = val.readableBytes();
                long newLen = (int)Math.max(off + rangeValLen, oldLen);
                // newLen will be checked against max len in makeStringValue()
                if (val.capacity() < newLen) {
                    val.capacity((int)newLen);
                }
                if (off > oldLen) {
                    // pad with zero bytes until the offset
                    val.setZero(oldLen, (int)off - oldLen);
                }
                return val.setBytes((int)off, rangeVal, 0, rangeValLen)
                    .writerIndex((int)newLen);
            },
            (val) -> new IntegerRedisMessage(
                val != null ? val.readableBytes() : 0));
    }

    public RedisMessage handleIncr(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doIncrBy(cmd.args[0], 1, true);
    }

    public RedisMessage handleIncrBy(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final long arg = Utils.byteBufToLong(cmd.args[1]);
        return doIncrBy(cmd.args[0], arg, true);
    }

    public RedisMessage handleDecr(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doIncrBy(cmd.args[0], 1, false);
    }

    public RedisMessage handleDecrBy(RedisClientContext client,
    RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final long arg = Utils.byteBufToLong(cmd.args[1]);
        return doIncrBy(cmd.args[0], arg, false);
    }

    public RedisMessage handleIncrByFloat(RedisClientContext client,
    RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        final double arg = Utils.byteBufToDouble(cmd.args[1]);
        return doGetSetString(cmd.args[0], val -> Utils.incrByFloat(val, arg),
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

        return val[0] != null ?
            new FullBulkStringRedisMessage(getStrVal(val[0])) :
            FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleMSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return handleMSet(client, cmd, false);
    }

    public RedisMessage handleMSetNX(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return doWithRetries(() -> handleMSet(client, cmd, true));
    }

    public RedisMessage handleGetEx(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkNumArgs(cmd, 1, 3);
        if (cmd.args.length == 1) {
            return doGetString(cmd.args[0]);
        }

        TTLMode ttlMode;
        ByteBuf expArg;

        String modeArg = Utils.byteBufToString(cmd.args[1]);
        if (modeArg.equalsIgnoreCase(TTLMode.EX.name())) {
            ttlMode = TTLMode.EX;
            expArg = cmd.args[2];
        } else if (modeArg.equalsIgnoreCase(TTLMode.PX.name())) {
            ttlMode = TTLMode.PX;
            expArg = cmd.args[2];
        } else if (modeArg.equalsIgnoreCase(TTLMode.EXAT.name())) {
            ttlMode = TTLMode.EXAT;
            expArg = cmd.args[2];
        } else if (modeArg.equalsIgnoreCase(TTLMode.PXAT.name())) {
            ttlMode = TTLMode.PXAT;
            expArg = cmd.args[2];
        } else if (modeArg.equalsIgnoreCase("PERSIST")) {
            if (cmd.args.length > 2) {
                throw RedisResponseException.syntaxError();
            }
            ttlMode = null;
            expArg = null;
        } else {
            throw RedisResponseException.syntaxError();
        }

        final long expTime = makeExpTime(expArg, ttlMode, cmd.name, true);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        return doWithRetries(() -> {
            RedisValueInfo oldVal = doGet(keyInfo);
            if (!oldVal.isValid()) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            if (!getValueType(oldVal.val).equals(TYPE_STRING)) {
                throw RedisResponseException.wrongType();
            }
            // small optimization for PERSIST option
            if (ttlMode == null && oldVal.exp == NO_EXP) {
                return valInfoToResp(oldVal);
            }
            RedisValueInfo newVal = RedisValueInfo.create(oldVal.val, null,
                expTime);
            doChkSet(keyInfo, newVal, oldVal);
            return valInfoToResp(newVal);
        });
    }

}
