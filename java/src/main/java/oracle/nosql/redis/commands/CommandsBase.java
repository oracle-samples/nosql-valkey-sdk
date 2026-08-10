/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
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
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.valkey.CommandHandlers.CommandHandler;
import oracle.nosql.valkey.NoSQLRedisServer;
import oracle.nosql.valkey.RawCommand;
import oracle.nosql.valkey.RedisResponseException;
import oracle.nosql.valkey.RedisResponseException.ErrorPrefix;
import oracle.nosql.valkey.RedisServerConfig;
import oracle.nosql.valkey.util.PreparedStatementCache;
import oracle.nosql.valkey.util.Utils;
import oracle.nosql.valkey.util.Utils.ThrowingConsumer;
import oracle.nosql.valkey.util.Utils.ThrowingFunction;

import static oracle.nosql.redis.util.Utils.*;

public abstract class CommandsBase {

    // TTLMode == null means no expiration.
    enum TTLMode {
        EX, // Duration in seconds
        EXAT, // Unix timestamp in seconds
        PX, // Duration in ms
        PXAT, // Unix timestamp in ms
        KEEP_TTL // Keep existing TTL of the key
    }

    public enum SetOpt {
        NX,
        XX
    }

    static final int KEY_MAX_SIZE = 128 * 1024;
    // Same length as SHA-256 hash in base64 which is used for long keys. Note
    // that for child tables like valkey.lists and valkey.hashes, the primary key
    // includes the parent key, so we have to leave some space there because of
    // Cloud PK size limitation.
    static final int SIMPLE_KEY_MAX = 44;

    static final char STR_KEY_PFX = 'T';
    static final char STR_VAL_PFX = STR_KEY_PFX;
    static final char BIN_KEY_PFX = 'B';
    static final char BIN_VAL_PFX = BIN_KEY_PFX;
    static final char HASH_PFX = 'H';

    static final String FLD_SLOT = "slot";
    static final String FLD_ID = "id";
    static final String FLD_KEY = "key";
    static final String FLD_VALUE = "value";
    static final String FLD_DATA = "data";
    // Used to retrieve row version in query.
    static final String FLD_VER = "ver";
    // Some result from query.
    static final String FLD_RES = "res";
    static final String KEY_DATA = FLD_DATA;
    static final String KEY_SCAN_ID = "scanId";
    static final String KEY_EXP = "exp";
    static final String VALUE_TYPE = "type";
    static final String VALUE_DATA = FLD_DATA;

    static final String SQL_DECLARE = "DECLARE ";
    static final String SQL_WHERE = "WHERE ";
    static final String SQL_RETURNING = " RETURNING ";
    static final String SQL_SLOT = "$slot";
    static final String SQL_KEY_ID = "$id";
    static final String SQL_KEY_IDS = "$ids";
    static final String DECL_KEY_ID =
        SQL_DECLARE + "$slot INTEGER; $id STRING; ";
    static final String DECL_KEY_IDS =
        SQL_DECLARE + "$slot INTEGER; $ids ARRAY(STRING); ";
    static final String R_PK_COND = "$r.slot = $slot AND $r.id = $id ";
    static final String WHERE_R_PK_COND = SQL_WHERE + R_PK_COND;
    static final String R_PKS_COND =
        "$r.slot = $slot AND $r.id IN $ids[]";
    static final String WHERE_R_PKS_COND = SQL_WHERE + R_PKS_COND;

    static final String NOT_EXPIRED =
        "(NOT EXISTS $r.key.exp OR $r.key.exp > current_time_millis()) ";
    static final String AND_NOT_EXPIRED = "AND " + NOT_EXPIRED;

    static final String ERR_NO_SINGLE_RES =
        "Expected single result, got multiple";
    static final String ERR_NO_SINGLE_RES_EMPTY =
        "Expected single result, got none";
    static final String ERR_WM_FAIL =
        "Unsuccessful result from writeMultiple";
    static final String ERR_UNCONDITIONAL_PUT_FAIL =
        "Unconditional put operation failed";
    static final String ERR_MISSING_EXISTING_VAL =
        "Missing existing value or version in result";

    public static final String TYPE_STRING = "string";
    public static final String TYPE_LIST = "list";
    public static final String TYPE_SET = "set";
    public static final String TYPE_ZSET = "zset";
    public static final String TYPE_HASH = "hash";
    public static final String TYPE_STREAM = "stream";
    public static final String TYPE_JSON = "ReJSON-RL";

    static final int EXPIRED = 0;
    static final int NO_EXP = -1;
    static final int KEEP_TTL = -2;

    // WriteMultipleRequest can do max of 50 ops.
    static final int MAX_WM_CNT = 50;

    static final String ERR_TOO_MANY_KEYS = String.format(
        "Atomic update of more than %s keys is not supported",
        MAX_WM_CNT);

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
        final int slot;
        final boolean isBin;

        // One potential issue: if expiration is given as TTL (not expiry
        // timestamp) and put takes several tries (because of version
        // conflicts, see doSet(), is it still ok to compute expire timestamp
        // only once before the whole operation rather than before each retry?
        // (considering HTTP requests may take on the order of 100ms for a
        // slower network)

        RedisKeyInfo(int slot, String id, String data, boolean isBin,
            long exp) {
            this.slot = slot;
            this.id = id;
            this.data = data;
            this.isBin = isBin;
            this.exp = exp;
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

        private RedisValueInfo(MapValue val, oracle.nosql.driver.Version ver,
            long exp) {
            this.val = val;
            this.ver = ver;
            this.exp = exp;
        }

        static boolean isTimeExpired(long expTime) {
            return expTime >= 0 && expTime < System.currentTimeMillis();
        }

        // We use value EXPIRED (0) to represent expired keys and avoid
        // checking the clock multiple times, since the exact expiration time
        // doesn't matter if it is already in the past. Note that we may still
        // need val since some commands accept exp. times past expiration as
        // input (this simplifies handling of those cases since we can avoid
        // performing delete on those keys).
        static RedisValueInfo create(MapValue val,
            oracle.nosql.driver.Version ver, long exp) {
            return new RedisValueInfo(val, ver,
                isTimeExpired(exp) ? EXPIRED : exp);
        }

        static RedisValueInfo createExpired(oracle.nosql.driver.Version ver) {
            return new RedisValueInfo(null, ver, EXPIRED);
        }

        boolean exists() {
            return ver != null;
        }
        
        boolean isValid() {
            return val != null && exp != EXPIRED;
        }

        // the key exists but has expired
        boolean isExpired() {
            return exp == EXPIRED && ver != null;
        }
    }

    // Scan that uses query, all current scans will extend this.
    static abstract class QueryScan extends Scan {
        protected QueryRequest qReq;
        protected QueryIterableResult qir;

        QueryScan(CommandsBase cmds, RedisKeyInfo keyInfo, long cursor,
            ByteBuf match, long count,
            Utils.ThrowingPredicate<MapValue,RedisResponseException> pred)
            throws RedisResponseException {
            super(cmds, keyInfo, cursor, match, count, pred);
        }

        abstract String getSQLScan();

        // Suppress resource leak warning because qReq.set... methods
        // below return the same instance.
        @SuppressWarnings("resource")
        @Override
        Iterator<MapValue> startScan() throws RedisResponseException {
            PreparedStatement pStmt = pstmtCache.getByRef(getSQLScan());
            if (keyInfo != null) {
                pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
                pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
            }

            pStmt.setVariable("$scanId", new LongValue(startScanId));

            qReq = new QueryRequest().setPreparedStatement(pStmt)
                .setLimit(count + 1);
            qir = nosqlHandle.queryIterable(qReq);
            return qir.iterator();
        }

        public void close() {
            if (qir != null) {
                qir.close();
                qir = null;
            }
            if (qReq != null) {
                qReq.close();
                qReq = null;
            }
        }

    }

    protected final NoSQLHandle nosqlHandle;
    protected final RedisServerConfig config;
    protected final PreparedStatementCache pstmtCache;

    public CommandsBase(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        this.nosqlHandle = nosqlHandle;
        this.config = config;
        this.pstmtCache = pstmtCache;
    }

    // The hash slot is the least significant 14 bits of CRC16 of the key. But
    // if we locate "{...}", and there are bytes between '{' and '}', we only
    // hash those bytes. The data between '{' and '}' is called a hash tag.
    // Note that, like Redis Cluster, we look for hash tag regardless whether
    // they key is text or binary.
    // Based on keyHashSlot in
    // https://github.com/redis/redis/blob/unstable/src/cluster.h
    private static int getKeyHashSlot(ByteBuf key) {
        int len = key.readableBytes();
        int i = key.indexOf(0, len, (byte)'{');
        if (i != -1) {
            i++;
            int j = key.indexOf(i, len, (byte)'}');
            if (j != -1 && j != i) {
                key = key.slice(i, j - i);
            }
        }
        return Utils.crc16(key) & 0x3FFF;
    }

    static PutRequest makePutReqForSet(RedisKeyInfo keyInfo, long exp,
        MapValue val, RedisValueInfo oldVal) {
        assert val != null;
        boolean skipTTL = false;

        // If exp = KEEP_TTL, we need to determine the actual exp value to set:
        // it will be either exp from existing key if any, or NO_EXP if the key
        // does not exist or we are doing unconditional set.
        // Also, if exp = KEEP_TTL and the key exists, we can skip setting the
        // actual row TTL (not sure if this provides any performance savings).
        // In all other cases, we must set TTL.
        if (exp == KEEP_TTL) {
            if (oldVal == null || !oldVal.isValid()) {
                exp = NO_EXP;
            } else {
                exp = oldVal.exp;
                skipTTL = true;
            }
        }

        MapValue row = new MapValue().put(FLD_SLOT, keyInfo.slot)
            .put(FLD_ID, keyInfo.id)
            .put(FLD_KEY, makeRedisKey(keyInfo, exp))
            .put(FLD_VALUE, val);
        PutRequest putReq = new PutRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setValue(row);

        // Redis default for SET command is to remove expiration from
        // existing key unless KEEPTTL is specified.
        // In case exp is already in the past (exp < currTime), we follow
        // Redis's behavior and treat the key as already expired. This will be
        // already indicated by the "exp" field inside the key, but we also
        // set actual TTL to the allowed minimum of 1 hour (see
        // Utils.getPositiveTTL).
        if (!skipTTL) {
            putReq.setTTL(exp == NO_EXP ?
                TimeToLive.DO_NOT_EXPIRE : getPositiveTTL(exp));
        }

        if (oldVal != null) {
            // Conditional put is conditional on existing version if any
            // (including expired row) or ifAbsent if old value doesn't exist.
            if (oldVal.ver == null) {
                putReq.setOption(PutRequest.Option.IfAbsent);
            } else {
                putReq.setOption(PutRequest.Option.IfVersion);
                putReq.setMatchVersion(oldVal.ver);
            }
        }

        return putReq;
    }

    private void doSetAlways(RedisKeyInfo keyInfo, MapValue val, long exp)
        throws RedisResponseException {
        PutRequest putReq = makePutReqForSet(keyInfo, exp, val, null);
        try {
            PutResult putRes = nosqlHandle.put(putReq);
            if (putRes.getVersion() == null) {
                throw RedisResponseException.nosql(ERR_UNCONDITIONAL_PUT_FAIL);
            }
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    // First try IfAbsent. If there is existing row, check if it's expired. If
    // not, NX fails. Otherwise, we can retry put, but condition it on
    // the existing version instead.
    private boolean doSetNX(RedisKeyInfo keyInfo, MapValue val, long exp)
        throws RedisResponseException {
        RedisValueInfo oldVal = RedisValueInfo.NONE;
        // TODO: Use doWithRetries() for this, perhaps allow storing custom
        // value in RedisRetryException, as well as callback to doWithRetries
        // that takes that value.
        for(int i = 0; i < config.getMaxAtomicRetries(); i++) {
            PutRequest putReq = makePutReqForSet(keyInfo, exp, val, oldVal);
            putReq.setReturnRow(true);

            try {
                PutResult putRes = nosqlHandle.put(putReq);
                if (putRes.getVersion() != null) {
                    return true;
                }

                MapValue existingRow = putRes.getExistingValue();
                oracle.nosql.driver.Version existingVer =
                    putRes.getExistingVersion();

                if (existingRow == null) {
                    if (existingVer != null) {
                        throw RedisResponseException.nosql(
                            "Result contains version for non-existent row");
                    }
                    // If we tried IfAbsent and failed, there must be existing
                    // row.
                    if (putReq.getOption() == PutRequest.Option.IfAbsent) {
                        throw RedisResponseException.nosql(
                            ERR_MISSING_EXISTING_VAL);
                    }
                    // There is a small chance that the DB has expired the row
                    // before this try, so we do IfAbsent again on the next
                    // try.
                    continue;
                }

                if (existingVer == null) {
                    throw RedisResponseException.nosql(
                        ERR_MISSING_EXISTING_VAL);
                }

                if (!isRowExpired(existingRow)) {
                    return false;
                }

                oldVal = RedisValueInfo.createExpired(existingVer);
            } catch (NoSQLException ex) {
                throw RedisResponseException.nosql(ex);
            }
        }

        throw failedAtomicRetries();
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
    
    static void chkSingleResult(List<?> res) throws RedisResponseException {
        if (res.size() != 1) {
            throw RedisResponseException.nosql(res.size() != 0 ?
                ERR_NO_SINGLE_RES : ERR_NO_SINGLE_RES_EMPTY);
        }
    }

    static void chkMaxNumResults(List<?> res, long maxCnt)
        throws RedisResponseException {
        if (res.size() > maxCnt) {
            throw RedisResponseException.nosql(
                String.format("Expected at most %d results, got %d", maxCnt,
                    res.size()));
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
    static RedisKeyInfo makeRedisKeyInfo(ByteBuf buf, long exp)
        throws RedisResponseException {
        assert buf != null;

        if (buf.readableBytes() > KEY_MAX_SIZE) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "key exceeds maximum allowed size");
        }

        boolean isBin = !ByteBufUtil.isText(buf, CharsetUtil.UTF_8);
        String data = isBin ?
            base64encode(buf) : buf.toString(CharsetUtil.UTF_8);

        String id = data.length() <= SIMPLE_KEY_MAX ?
            (isBin ? BIN_KEY_PFX : STR_KEY_PFX) + data :
            HASH_PFX + Utils.createDigest(buf);

        return new RedisKeyInfo(getKeyHashSlot(buf), id, data, isBin, exp);
    }

    static RedisKeyInfo makeRedisKeyInfo(ByteBuf buf)
        throws RedisResponseException {
        return makeRedisKeyInfo(buf, NO_EXP);
    }

    static RedisKeyInfo[] makeRedisMultiKeyInfo(ByteBuf[] keys, int off,
        int cnt, int step) throws RedisResponseException {
        RedisKeyInfo[] res = new RedisKeyInfo[cnt];
        int slot = -1;
        
        for(int i = 0; i < cnt; i++) {
            RedisKeyInfo keyInfo = makeRedisKeyInfo(keys[off + (i * step)]);
            if (slot == -1) {
                slot = keyInfo.slot;
            } else if (keyInfo.slot != slot) {
                throw RedisResponseException.crossSlot();
            }
            res[i] = keyInfo;
        }

        return res;
    }

    static RedisKeyInfo[] makeRedisMultiKeyInfo(ByteBuf[] keys, int off,
        int cnt) throws RedisResponseException {
        return makeRedisMultiKeyInfo(keys, off, cnt, 1);
    }

    static ArrayValue makeKeyIdsValue(RedisKeyInfo[] keyInfos) {
        return new ArrayValue().addAll(
            Arrays.stream(keyInfos).map(val -> new StringValue(val.id)));
    }

    static MapValue makePrimaryKey(RedisKeyInfo keyInfo) {
        return new MapValue().put(FLD_SLOT, keyInfo.slot)
            .put(FLD_ID, keyInfo.id);
    }

    // This overload takes exp separately from keyInfo, used for set methods
    // where different exp can be provided.
    static MapValue makeRedisKey(RedisKeyInfo keyInfo, long exp) {
        MapValue res = new MapValue().put(KEY_DATA,
            (keyInfo.isBin ? BIN_KEY_PFX : STR_KEY_PFX) + keyInfo.data)
            .put(KEY_SCAN_ID, Scan.makeScanId(keyInfo));
        if (exp >= 0) {
            res.put(KEY_EXP, exp);
        }
        return res;
    }

    static MapValue makeRedisKey(RedisKeyInfo keyInfo) {
        return makeRedisKey(keyInfo, keyInfo.exp);
    }

    static MapValue rowToKey(MapValue row) throws RedisResponseException {
        return getMapField(row, FLD_KEY);
    }

    static MapValue rowToValue(MapValue row) throws RedisResponseException {
        return getMapField(row, FLD_VALUE);
    }

    // To avoid creating Version object when we only need FieldValue.
    static FieldValue rowToVerVal(MapValue row) throws RedisResponseException {
        FieldValue val = row.get(FLD_VER);
        if (val == null || !val.isBinary()) {
            throw RedisResponseException.corrupt("Invalid row version");
        }
        return val;
    }

    // Retrieves row version from the query result row, where it is returned
    // as "ver" field.
    static oracle.nosql.driver.Version rowToVer(MapValue row)
        throws RedisResponseException {
        return oracle.nosql.driver.Version.createVersion(
            rowToVerVal(row).getBinary());
    }

    static String getId(MapValue val) throws RedisResponseException {
        return getStringField(val, FLD_ID);
    }

    static String getValueType(MapValue val) throws RedisResponseException {
        return getStringField(val, VALUE_TYPE);
    }

    static String getData(MapValue val) throws RedisResponseException {
        return getStringField(val, KEY_DATA);
    }

    static ByteBuf keyToKeyBuf(MapValue key) throws RedisResponseException {
        return getStrVal(getData(key));
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

        if (!isBin) {
            return Unpooled.copiedBuffer(val.substring(1),
                CharsetUtil.UTF_8);
        }

        try {
            return Utils.base64decode(val.substring(1));
        } catch(IllegalArgumentException ex) {
            throw RedisResponseException.corrupt("Invalid base64 string");
        }
    }
    
    static String makeStrVal(ByteBuf buf) {
        boolean isText = ByteBufUtil.isText(buf, CharsetUtil.UTF_8);
        return isText ?
            STR_VAL_PFX + buf.toString(CharsetUtil.UTF_8) :
            BIN_VAL_PFX + Utils.base64encode(buf);
    }

    static boolean isKeyExpired(MapValue key) {
        long expTime = getExpTime(key);
        return expTime != NO_EXP && expTime < System.currentTimeMillis();
    }

    static boolean isRowExpired(MapValue row) throws RedisResponseException {
        return isKeyExpired(rowToKey(row));
    }

    static long makeExpTime(ByteBuf expArg, TTLMode mode, String cmdName,
        boolean chkPositive) throws RedisResponseException {
        if (mode == null) {
            return NO_EXP;
        } else if (mode == TTLMode.KEEP_TTL) {
            return KEEP_TTL;
        }

        // The below mimics error handling done in Redis.
        long exp = Utils.byteBufToLong(expArg);

        try {
            if (chkPositive && exp <= 0) {
                throw new ArithmeticException();
            }
            switch(mode) {
                case EX:
                    exp = Math.addExact(System.currentTimeMillis(),
                        Math.multiplyExact(exp, 1000));
                    break;
                case EXAT:
                    exp = Math.multiplyExact(exp, 1000);
                    break;
                case PX:
                    exp = Math.addExact(System.currentTimeMillis(), exp);
                    break;
                case PXAT:
                    break;
                default:
                    assert false;
                    break;
            }
        } catch(ArithmeticException ex) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                String.format("invalid expire time in '%s' command",
                    cmdName.toLowerCase()));
        }

        // avoid exp to clash with defined special negative values (NO_EXP and
        // KEEP_TTL)
        return exp > 0 ? exp : EXPIRED;
    }

    static long makeExpTime(ByteBuf expArg, TTLMode mode, String cmdName)
        throws RedisResponseException {
        return makeExpTime(expArg, mode, cmdName, false);
    }

    RedisResponseException failedAtomicRetries() {
        return new RedisResponseException(ErrorPrefix.NOSQL,
            "Failed to perform atomic read-update sequence after " +
                config.getMaxAtomicRetries() + " tries");
    }

    <R> R doWithRetries(
        Utils.ThrowingNoArgFunction<R, RedisResponseException> func)
        throws RedisResponseException {
        return Utils.doWithRetries(func, config.getMaxAtomicRetries());
    }

    boolean isCollectionType() {
        return false;
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

            return RedisValueInfo.create(rowToValue(row), getRes.getVersion(),
                getExpTime(rowToKey(row)));
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    RedisValueInfo doGet(ByteBuf keyBuf) throws RedisResponseException {
        return doGet(makeRedisKeyInfo(keyBuf));
    }

    // Note that we cannot handle XX here without reading the row first. This
    // is because if the existing row contains expired key, putIfPresent will
    // still succeed and overwrite the row. The best way to handle XX is
    // through SQL UPDATE query.
    boolean doSet(RedisKeyInfo keyInfo, MapValue val, long exp, boolean isNX)
        throws RedisResponseException {
        if (isNX) {
            return doSetNX(keyInfo, val, exp);
        }
        doSetAlways(keyInfo, val, exp);
        return true;
    }

    // To use with possible retries.
    void doChkSet(RedisKeyInfo keyInfo, RedisValueInfo newVal,
        RedisValueInfo oldVal) throws RedisResponseException {
        PutRequest putReq = makePutReqForSet(keyInfo, newVal.exp,
            newVal.val, oldVal);
        try {
            PutResult putRes = nosqlHandle.put(putReq);
            if (putRes.getVersion() == null) {
                throw new Utils.RedisRetryException();
            }
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
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
                throw RedisResponseException.nosql(ERR_MISSING_EXISTING_VAL);
            }
            RedisValueInfo res = RedisValueInfo.create(rowToValue(row), ver,
                getExpTime(rowToKey(row)));
            return res.isValid() ? res : RedisValueInfo.NONE;
        }
        catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    protected RedisResponseException processNoSQLException(Exception ex) {
        if (ex instanceof RedisResponseException) {
            return (RedisResponseException)ex;
        }
        return RedisResponseException.nosql(ex);
    }

    // applyRow() will return true if we are done and should not process
    // remaining rows, false otherwise.
    // Return value - true if applyRow() returned true, false otherwise.
    protected boolean processQuery(PreparedStatement pStmt,
        ThrowingFunction<MapValue, Boolean, RedisResponseException> applyRow)
        throws RedisResponseException {

        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                for(MapValue row : qir) {
                    if (applyRow.apply(row)) {
                        return true;
                    }
                }
            }
        } catch(Exception ex) {
            throw processNoSQLException(ex);
        }

        return false;
    }

    protected void processQuery(PreparedStatement pStmt,
        ThrowingConsumer<MapValue, RedisResponseException> acceptRow)
        throws RedisResponseException {
        processQuery(pStmt, (row) -> {
            acceptRow.accept(row);
            return false;
        });
    }

    protected void processQuery(PreparedStatement pStmt)
        throws RedisResponseException {
        processQuery(pStmt, (row) -> {});
    }

    protected MapValue singleRowQuery(PreparedStatement pStmt)
        throws RedisResponseException {
        final MapValue [] res = new MapValue[1];
        if (!processQuery(pStmt, row -> {
            if (res[0] != null) {
                throw RedisResponseException.nosql(ERR_NO_SINGLE_RES);
            }
            res[0] = row;
            return true;
        })) {
            throw RedisResponseException.nosql("Missing query result");
        };
        return res[0];
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

    public void cleanupElemsTable() {
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {}

}
