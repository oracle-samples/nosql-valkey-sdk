/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.function.Predicate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.NoSQLRedisServer;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;
import oracle.nosql.redis.util.Utils.ThrowingFunction;

public abstract class CommandsBase {

    // TTLMode == null means no expiration.
    enum TTLMode {
        EX, // Duration in seconds
        EXAT, // Unix timestamp in seconds
        PX, // Duration in ms
        PXAT, // Unix timestamp in ms
        KEEP_TTL // Keep existing TTL of the key
    }

    public static enum SetOpt {
        NX,
        XX
    }

    static final int SIMPLE_KEY_MAX = 63;

    static final char STR_KEY_PFX = 'T';
    static final char STR_VAL_PFX = STR_KEY_PFX;
    static final char BIN_KEY_PFX = 'B';
    static final char BIN_VAL_PFX = BIN_KEY_PFX;
    static final char HASH_PFX = 'H';

    static final String FLD_ID = "id";
    static final String FLD_KEY = "key";
    static final String FLD_VALUE = "value";
    static final String FLD_DATA = "data";
    static final String KEY_DATA = FLD_DATA;
    static final String KEY_SCAN_ID = "scanId";
    static final String KEY_EXP = "exp";
    static final String VALUE_TYPE = "type";
    static final String VALUE_DATA = FLD_DATA;

    public static final String TYPE_STRING = "string";
    public static final String TYPE_LIST = "list";
    public static final String TYPE_SET = "set";
    public static final String TYPE_ZSET = "zset";
    public static final String TYPE_HASH = "hash";
    public static final String TYPE_STREAM = "stream";

    static final int NO_EXP = -1;
    static final int KEEP_TTL = -2;

    //static final int ATOMIC_SET_TRIES = 10;
    static final int ATOMIC_SET_TRIES = 10000000;

    static final SimpleStringRedisMessage okReply =
        new SimpleStringRedisMessage("OK");
    static final IntegerRedisMessage zeroReply = new IntegerRedisMessage(0);
    static final IntegerRedisMessage oneReply = new IntegerRedisMessage(1);
    static final IntegerRedisMessage minusOneReply =
        new IntegerRedisMessage(-1);

    // Used mostly when writing key-value pair.
    static class RedisKeyInfo {
        final String id;
        final String data;
        long exp;
        final boolean isBin;

        // One potential issue: if expiration is given as TTL (not expiry
        // timestamp) and put takes several tries (because of version
        // conflicts, see doSet), is it still ok to compute expire timestamp
        // only once before the whole operation rather than before each retry?
        // (considering HTTP requests may take on the order of 100ms for a
        // slower network)

        RedisKeyInfo(String id, String data, boolean isBin, long exp) {
            this.id = id;
            this.data = data;
            this.isBin = isBin;
            this.exp = exp;
        }

        RedisKeyInfo(String id, String data, boolean isBin) {
            this(id, data, isBin, NO_EXP);
        }
    }

    static class RedisValueInfo {
        final MapValue val;
        final oracle.nosql.driver.Version ver;
        final long exp;

        // Represents non-existing row or when we want to unconditionally
        // overwrite existing row.
        static final RedisValueInfo NONE = new RedisValueInfo(null, null,
            NO_EXP);

        RedisValueInfo(MapValue val, oracle.nosql.driver.Version ver,
            long exp) {
            this.val = val;
            this.ver = ver;
            this.exp = exp;
        }

        boolean exists() {
            return val != null;
        }

        boolean isValid(long currTime) {
            return val != null && (exp == NO_EXP || currTime <= exp);
        }

        boolean isValid() {
            return val != null && (exp == NO_EXP ||
                System.currentTimeMillis() <= exp);
        }

        // the key exists but has expired
        boolean isExpired(long currTime) {
            return val != null && exp != NO_EXP && currTime > exp;
        }

        boolean isExpired() {
            return isExpired(System.currentTimeMillis());
        }
    }

    protected NoSQLHandle nosqlHandle;
    protected PreparedStatementCache pstmtCache;

    CommandsBase(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        this.nosqlHandle = nosqlHandle;
        this.pstmtCache = pstmtCache;
    }

    static void chkExactNumArgs(RawCommand cmd, int numArgs)
        throws RedisResponseException {
        if ((cmd.args == null && numArgs != 0) ||
            (cmd.args != null && cmd.args.length != numArgs)) {
            throw RedisResponseException.numArgs(cmd.name);
        }
    }

    // minNumArgs > 0
    static void chkMinNumArgs(RawCommand cmd, int minNumArgs)
        throws RedisResponseException {
        if (cmd.args == null || cmd.args.length < minNumArgs) {
            throw RedisResponseException.numArgs(cmd.name);
        }
    }

    static void chkNumArgs(RawCommand cmd, int minNumArgs, int maxNumArgs)
        throws RedisResponseException {
        int numArgs = cmd.args != null ? cmd.args.length : 0;
        if (numArgs < minNumArgs || numArgs > maxNumArgs) {
            throw RedisResponseException.numArgs(cmd.name);
        }
    }

    static void chkNotSet(Object o) throws RedisResponseException {
        if (o != null) {
            throw RedisResponseException.syntaxError();
        }
    }

    static void chkDbIdx(long dbIdx) throws RedisResponseException {
        if (dbIdx < 0 || dbIdx >= 16) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "DB index is out of range");
        }
    }

    //TODO: isText() takes time. We should avoid calling it if possible.
    //Maybe this could be combined with conversion to UTF-8, that is if it
    //fails, we consider the key binary and do base64 encoding. This will
    //work well when most of the keys are UTF-8. In any case, I suspect the
    //time taken by isText is comparable to the actual UTF-8 conversion.
    //Another way would be to use isText() only for short keys and always
    //base64-encode longer keys.
    static RedisKeyInfo makeRedisKeyInfo(ByteBuf buf, long exp) {
        boolean isBin = !ByteBufUtil.isText(buf, CharsetUtil.UTF_8);
        if (isBin) {
            buf = Base64.encode(buf);
        }
        
        String data = buf.toString(CharsetUtil.UTF_8);
        String id = data.length() <= SIMPLE_KEY_MAX ?
            (isBin ? BIN_KEY_PFX : STR_KEY_PFX) + data :
            HASH_PFX + Utils.createDigest(buf);
        
        return new RedisKeyInfo(id, data, isBin, exp);
    }

    static RedisKeyInfo makeRedisKeyInfo(ByteBuf buf) {
        return makeRedisKeyInfo(buf, NO_EXP);
    }

    static String makeKeyId(ByteBuf buf) {
        return makeRedisKeyInfo(buf).id;
    }

    static MapValue makePrimaryKey(RedisKeyInfo keyInfo) {
        return new MapValue().put(FLD_ID, keyInfo.id);
    }

    static MapValue makePrimaryKey(ByteBuf buf) {
        return makePrimaryKey(makeRedisKeyInfo(buf));
    }

    // This overload takes exp separately from keyInfo, used for set methods
    // where different exp can be provided.
    static MapValue makeRedisKey(RedisKeyInfo keyInfo, long exp) {
        MapValue res = new MapValue().put(KEY_DATA,
            (keyInfo.isBin ? BIN_KEY_PFX : STR_KEY_PFX) + keyInfo.data)
            .put(KEY_SCAN_ID, Scan.makeScanId(keyInfo));
        if (exp > 0) {
            res.put(KEY_EXP, exp);
        }
        return res;
    }

    static MapValue makeRedisKey(RedisKeyInfo keyInfo) {
        return makeRedisKey(keyInfo, keyInfo.exp);
    }

    static String getStringField(MapValue mapVal, String fieldName,
        boolean allowNull) throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (allowNull && fldVal != null && fldVal.isAnyNull()) {
            return null;
        }
        if (fldVal == null || !fldVal.isString()) {
            throw RedisResponseException.corrupt(
                "Missing or invalid field " + fieldName);
        }

        return fldVal.getString();
    }

    static String getStringField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        return getStringField(mapVal, fieldName, false);
    }

    static MapValue rowToKey(MapValue row) throws RedisResponseException {
        FieldValue key = row.get(FLD_KEY);
        if (key == null || !key.isMap()) {
            throw RedisResponseException.corrupt("Invalid key");
        }
        return key.asMap();
    }

    static MapValue rowToValue(MapValue row) throws RedisResponseException {
        FieldValue val = row.get(FLD_VALUE);
        if (val == null || !val.isMap()) {
            throw RedisResponseException.corrupt("Invalid value");
        }
        return val.asMap();
    }

    static String getValueType(MapValue val) throws RedisResponseException {
        return getStringField(val, VALUE_TYPE);
    }
    
    static String getData(MapValue val) throws RedisResponseException {
        FieldValue fldData = val.get(KEY_DATA);
        if (fldData == null || !fldData.isString()) {
            throw RedisResponseException.corrupt(
                "Missing or invalid data field");
        }
        return fldData.asString().getValue();
    }

    static long getExpTime(MapValue key) {
        FieldValue expFld = key.get(KEY_EXP);
        if (expFld == null || expFld.isAnyNull()) {
            return NO_EXP;
        }
        if (!expFld.isLong()) {
            // log error
            return 0; // tread as if already expired
        }
        return expFld.asLong().getValue();
    }

    static ByteBuf getStrVal(String val) throws RedisResponseException {
        if (val.isEmpty()) {
            throw RedisResponseException.corrupt("Invalid value");
        }

        char pfx = val.charAt(0);
        boolean isBin = pfx == BIN_VAL_PFX;
        if (!isBin && pfx != STR_VAL_PFX) {
            throw RedisResponseException.corrupt("Invalid value");
        }
        
        ByteBuf res = Unpooled.copiedBuffer(val.substring(1),
            CharsetUtil.UTF_8);
        if (isBin) {
            try {
                res = Base64.decode(res);
            } catch(Exception ex) {
                throw RedisResponseException.corrupt(
                    "Invalid base64 encoding of value");
            }
        }

        return res;
    }
    
    static String makeStrVal(ByteBuf buf) {
        boolean isText = ByteBufUtil.isText(buf, CharsetUtil.UTF_8);
        if (!isText) {
            buf = Base64.encode(buf);
        }

        return (isText ? STR_VAL_PFX : BIN_VAL_PFX) +
            buf.toString(CharsetUtil.UTF_8);
    }

    static boolean isKeyExpired(MapValue key) {
        long expTime = getExpTime(key);
        return expTime != NO_EXP && expTime < System.currentTimeMillis();
    }

    static boolean isRowExpired(MapValue row) throws RedisResponseException {
        return isKeyExpired(rowToKey(row));
    }

    static long makeExpTime(ByteBuf buf, TTLMode mode)
        throws RedisResponseException {
        if (mode == null) {
            return NO_EXP;
        } else if (mode == TTLMode.KEEP_TTL) {
            return KEEP_TTL;
        }

        long exp = Utils.byteBufToLong(buf);

        switch(mode) {
            case EX:
                return System.currentTimeMillis() + exp * 1000;
            case EXAT:
                return exp * 1000;
            case PX:
                return System.currentTimeMillis() + exp;
            case PXAT:
                return exp;
            default:
                assert false;
                return 0;
        }
    }

    RedisValueInfo doGet(RedisKeyInfo keyInfo) throws RedisResponseException {
        try {
            GetResult getRes = nosqlHandle.get(new GetRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setKey(makePrimaryKey(keyInfo)));
            MapValue row = getRes.getValue();
            if (row == null) {
                return RedisValueInfo.NONE;
            }
            if (getRes.getVersion() == null) {
                throw new RedisResponseException(ErrorPrefix.CORRUPT,
                    "null version for existing row");
            }
            return new RedisValueInfo(rowToValue(row), getRes.getVersion(),
                getExpTime(rowToKey(row)));
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    RedisValueInfo doGet(ByteBuf keyBuf) throws RedisResponseException {
        return doGet(makeRedisKeyInfo(keyBuf));
    }

    boolean doSet(RedisKeyInfo keyInfo, MapValue val, long exp, SetOpt setOpt)
        throws RedisResponseException {
        MapValue row = new MapValue().put(FLD_ID, keyInfo.id)
            .put(FLD_KEY, makeRedisKey(keyInfo, exp))
            .put(FLD_VALUE, val);
        PutRequest putReq = new PutRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setValue(row);

        long currTime = System.currentTimeMillis();
        // see doGetSet()
        if (exp == NO_EXP || exp >= currTime) {
            putReq.setTTL(exp == NO_EXP ? TimeToLive.DO_NOT_EXPIRE :
                TimeToLive.fromExpirationTime(exp, currTime));
        }

        if (setOpt != null) {
            putReq.setOption(setOpt == SetOpt.NX ?
                PutRequest.Option.IfAbsent : PutRequest.Option.IfPresent);
        }
    
        try {
            PutResult putRes = nosqlHandle.put(putReq);
            return putRes.getVersion() != null;
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    boolean doSet(ByteBuf keyBuf, MapValue val, long exp, SetOpt setOpt)
        throws RedisResponseException {
        return doSet(makeRedisKeyInfo(keyBuf, exp), val, exp, setOpt);
    }

    // All-purpose function for atomic get-set sequence.
    // needSet - predicate to determine if set should proceed.  E.g. when
    // using Set.NX or SetOpt in StringCommands or TTLOpt in GenericCommands.
    // getNewVal - function to compute new value from the old value (it can
    // just return provided value for SET commands).  Note that it will also
    // determine new expiration time and set it as part of RedisValueInfo.
    // getResult - function to compute the result of the command based on old
    // and new values (mostly one or the other).
    // needOldVal - whether it is required to obtain an old value first, if
    // not, we can avoid extra request and use RedisValueInfo.NONE.  E.g. old
    // value is not required for unconditional SET commands.
    <R> R doGetSet(
        RedisKeyInfo keyInfo,
        Predicate<RedisValueInfo> needSet,
        ThrowingFunction<RedisValueInfo,RedisValueInfo,RedisResponseException>
            getNewVal,
        ThrowingBiFunction<RedisValueInfo, RedisValueInfo, R,
            RedisResponseException> getResult,
        boolean needOldVal) throws RedisResponseException {

        // Note that we have to check whether any existing
        // key has already expired and if so, treat it as non-existing.
        // This is relevant when using NX or XX or KEEPTTL (since we don't
        // want to keep TTL of an expired key). 
        // The loop below is used to make the whole operation atomic.  For
        // unconditional set operation, the loop below will perform
        // unconditional put with only one iteration.
        // Note that atomicity of such operations is required, e.g. to
        // implement the locking algorithm described here:
        // https://redis.io/commands/setnx/

        for(int i = 0; i < ATOMIC_SET_TRIES; i++) {
            RedisValueInfo oldVal = needOldVal ?
                doGet(keyInfo) : RedisValueInfo.NONE;

            long currTime = System.currentTimeMillis();
            boolean isValid = oldVal.isValid(currTime);

            if (!needSet.test(isValid ? oldVal : RedisValueInfo.NONE)) {
                return getResult.apply(oldVal, RedisValueInfo.NONE);
            }

            RedisValueInfo newVal = getNewVal.apply(
                isValid ? oldVal : RedisValueInfo.NONE);
            long exp = (!isValid && newVal.exp == KEEP_TTL) ?
                NO_EXP : newVal.exp;

            MapValue row = new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_KEY, makeRedisKey(keyInfo, exp))
                .put(FLD_VALUE, newVal.val);
            PutRequest putReq = new PutRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setValue(row);
    
            // Redis default for SET command is to remove expiration from
            // existing key unless KEEPTTL is specified.
            // In case exp is already in the past (exp < currTime), we follow
            // Redis's behavior and treat the key as already expired.  This
            // will be already indicated by the "exp" field inside the key and
            // we can avoid setting the row's TTL (so that we don't set it to
            // negative TTL).  We also don't set TTL if exp = KEEP_TTL.
            if (exp == NO_EXP || exp >= currTime) {
                putReq.setTTL(exp == NO_EXP ? TimeToLive.DO_NOT_EXPIRE :
                    TimeToLive.fromExpirationTime(exp, currTime));
            }
        
            // For atomicity, put is conditional on existing version if
            // any (including expired row) or ifAbsent if old value doesn't
            // exist but was needed (e.g. if using SetMode.NX).  Otherwise,
            // the put is unconditional.
            if (oldVal.ver != null) {
                putReq.setOption(PutRequest.Option.IfVersion);
                putReq.setMatchVersion(oldVal.ver);                        
            } else if (needOldVal) {
                putReq.setOption(PutRequest.Option.IfAbsent);
            }
            try {
                PutResult putRes = nosqlHandle.put(putReq);
                if (putRes.getVersion() != null) {
                    return getResult.apply(oldVal, newVal);
                }
            } catch(NoSQLException ex) {
                throw RedisResponseException.nosql(ex);
            }
        }

        throw new RedisResponseException(ErrorPrefix.NOSQL,
            "Failed to perform atomic get-set sequence after "
                + ATOMIC_SET_TRIES + " tries");
        
    }

    <R> R doGetSet(
        ByteBuf keyBuf,
        Predicate<RedisValueInfo> needSet,
        ThrowingFunction<RedisValueInfo,RedisValueInfo,RedisResponseException>
            getNewVal,
        ThrowingBiFunction<RedisValueInfo, RedisValueInfo, R,
            RedisResponseException> getResult,
        boolean needOldVal) throws RedisResponseException {
        return doGetSet(makeRedisKeyInfo(keyBuf), needSet, getNewVal,
            getResult, needOldVal);
    }

    RedisValueInfo doDelGetVal(RedisKeyInfo keyInfo)
        throws RedisResponseException {
        DeleteRequest delReq = new DeleteRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setKey(makePrimaryKey(keyInfo))
            .setReturnRow(true);
        try {
            DeleteResult delRes = nosqlHandle.delete(delReq);
            if (!delRes.getSuccess()) {
                return RedisValueInfo.NONE;
            }
            MapValue row = delRes.getExistingValue();
            oracle.nosql.driver.Version ver = delRes.getExistingVersion();
            if (row == null || ver == null) {
                throw RedisResponseException.corrupt(
                    "Delete result missing existing value or version");
            }
            RedisValueInfo res = new RedisValueInfo(rowToValue(row), ver,
                getExpTime(rowToKey(row)));
            return res.isValid() ? res : RedisValueInfo.NONE;
        }
        catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    // Atomic get-delete sequence, similar to doGetSet.  Note that even for
    // DEL command we do have to get old value to check its expiration, in
    // order to compute correct result.
    // beforeDelete - callback before deletion, will only be called if the key
    // exists. Throw if any error detected, in which case the deletion will not
    // proceed. Returns intermediate result of type I.
    // getResult - callback after deletion, can use existing value and
    // intermediate result from beforeDelete, returns final result of type R.
    <R, I> R doGetDel(RedisKeyInfo keyInfo,
        ThrowingFunction<RedisValueInfo, I, RedisResponseException>
            beforeDelete,
        ThrowingBiFunction<RedisValueInfo, I, R, RedisResponseException>
            getResult) throws RedisResponseException {
        DeleteRequest delReq = new DeleteRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setKey(makePrimaryKey(keyInfo));

        for(int i = 0; i < ATOMIC_SET_TRIES; i++) {
            RedisValueInfo oldVal = doGet(keyInfo);
            if (!oldVal.isValid()) {
                return getResult.apply(RedisValueInfo.NONE, null);
            }
            
            I iRes = beforeDelete.apply(oldVal);

            delReq.setMatchVersion(oldVal.ver);

            try {
                DeleteResult delRes = nosqlHandle.delete(delReq);
                if (delRes.getSuccess()) {
                    return getResult.apply(oldVal, iRes);
                }
            } catch(NoSQLException ex) {
                throw RedisResponseException.nosql(ex);
            }
        }

        throw new RedisResponseException(ErrorPrefix.NOSQL,
            "Failed to perform atomic delete after "
                + ATOMIC_SET_TRIES + " tries");
    }

    <R, I> R doGetDel(ByteBuf keyBuf,
        ThrowingFunction<RedisValueInfo, I, RedisResponseException>
            beforeDelete,
        ThrowingBiFunction<RedisValueInfo, I, R, RedisResponseException>
            getResult) throws RedisResponseException {
        return doGetDel(makeRedisKeyInfo(keyBuf), beforeDelete, getResult);
    }

    // Delete elements of the collection. Overriden for collection commands.
    protected void doDelElems(RedisKeyInfo keyInfo, RedisValueInfo valInfo)
        throws RedisResponseException {}

    // Overriden for collections. Will update TTL for collection element rows.
    protected void doSetElemsExp(RedisKeyInfo keyInfo, RedisValueInfo valInfo,
        TimeToLive ttl) throws RedisResponseException {}

    // Overriden for collections, where it will also copy collection elements.
    // The value returned may be different from the source.
    protected MapValue doCopy(RedisKeyInfo srcKeyInfo,
        RedisValueInfo srcValInfo, RedisKeyInfo dstKeyInfo)
        throws RedisResponseException {
        return srcValInfo.val;
    }

}
