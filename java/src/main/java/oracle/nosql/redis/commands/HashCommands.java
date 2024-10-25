package oracle.nosql.redis.commands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;

public class HashCommands extends CollectionCommandsBase {

    private static final int MAX_SMALL_HASH_ENT_SIZE = 10 * 1024;
    private static final int MAX_SMALL_HASH_SIZE = MAX_TXN_ELEM_CNT;
    //private static final int MAX_SMALL_HASH_ENT_SIZE = 10;
    //private static final int MAX_SMALL_HASH_SIZE = 5;

    private static final String HASH_TABLE_NAME = "redis.hashes";
    private static final String FLD_KEY_ID = "keyId";
    private static final String FLD_SMALL_VAL = "smallVal";
    private static final String FLD_FLD_VAL = "fldVal";

    private static String DT_STRING = "STRING";
    private static String DT_STRING_ARRAY = "ARRAY(STRING)";
    private static String IN_ARRAY_VAR2 = "IN $var2[]";
    private static String EQ_VAL_VAR2 = "= $var2";

    protected static final String SQL_ENTRY_IDS_FMT =
        "DECLARE $var1 STRING; $var2 %s; SELECT row_version($r) AS ver, " +
        "$r.key, $r.value, $h.keyId FROM redis $r LEFT OUTER JOIN " +
        "redis.hashes $h ON $r.id = $h.id AND $r.value.cid = $h.cid AND " +
        "($h.keyId IS NULL OR $h.keyId %s) WHERE $r.id = $var1 ";
    protected static final String SQL_ENTRY_IDS = String.format(
        SQL_ENTRY_IDS_FMT, DT_STRING_ARRAY, IN_ARRAY_VAR2);
    protected static final String SQL_ENTRY_ID = String.format(
            SQL_ENTRY_IDS_FMT, DT_STRING, EQ_VAL_VAR2);

    private static final String SQL_HGET_FMT =
        "DECLARE $var1 STRING; $var2 %s; SELECT $r.value, %s$h.value " +
        "AS fldVal FROM redis $r LEFT OUTER JOIN redis.hashes $h ON " +
        "$r.id = $h.id AND $r.value.cid = $h.cid WHERE $r.id = $var1 " +
        NOT_EXPIRED + "AND ($h.keyId IS NULL OR $h.keyId %s)";
    private static final String SQL_HGET_KEYVALS = String.format(SQL_HGET_FMT,
        DT_STRING_ARRAY, "$h.keyId, ", IN_ARRAY_VAR2);
    private static final String SQL_HGET_VAL = String.format(SQL_HGET_FMT,
        DT_STRING, "", EQ_VAL_VAR2);

    protected static final String SQL_SEL_ELEMS = String.format(
        SQL_SEL_ELEMS_FMT, HASH_TABLE_NAME);
    protected static final String SQL_DEL_ELEMS = String.format(
        SQL_DEL_ELEMS_FMT, HASH_TABLE_NAME);
    
    public static final String CMD_HSET = "HSET";
    public static final String CMD_HDEL = "HDEL";
    public static final String CMD_HLEN = "HLEN";
    public static final String CMD_HGET = "HGET";
    public static final String CMD_HMGET = "HMGET";

    // Because small hashes are used often, we use an optimization: before
    // a hash reaches certain size, we store it as single MapValue inside the
    // "smallVal" subfield of "value" field in the main redis table. Once
    // has reaches certain size, we convert it to multi-row format where each
    // entry is stored in a separate row in "redis.hashes" table and the
    // "value" field in main redis table stores only hash size under "len"
    // subfield. Note that subfields "smallVal" and "len" are exclusive.
    // In the header we use len = 0 to indicate absence of "len" field.
    // For simplicity, we use 2 events to convert the hash from "smallVal" to
    // multi-row format, whichever happens first:
    // 1) Hash size (number of fields) becomes at least MAX_SMALL_HASH_SIZE.
    // 2) Any hash entry becomes at least MAX_SMALL_HASH_ENT_SIZE bytes (for
    // simplicity, we use sum of key and value sizes as an estimate of the
    // entry size).
    // In future, we should make these values configurable. Once the hash is
    // in multi-row format, it will stay in multi-row format for its remaining
    // lifetime. Note that MAX_SMALL_HASH_SIZE cannot be greater than
    // MAX_TXN_ELEM_CNT. This is because conversion to multi-row format has
    // to be done atomically (in one transaction).
    protected static class HashHeader extends CollectionHeader {
        long len;
        MapValue smallVal;
        
        // Called when new hash is created.
        HashHeader() {
            this.smallVal = new MapValue();
        }

        HashHeader(String cid, MapValue smallVal, long len) {
            super(cid);
            assert smallVal != null ^ len != 0;
            this.smallVal = smallVal;
            this.len = len;
        }

        HashHeader(MapValue val) throws RedisResponseException {
            super(val);

            if (!getValueType(val).equals(TYPE_HASH)) {
                throw RedisResponseException.wrongType();
            }
            
            FieldValue fldSmallVal = val.get(FLD_SMALL_VAL);
            FieldValue fldLen = val.get(FLD_LEN);
            if (fldSmallVal != null && fldLen != null) {
                throw RedisResponseException.corrupt(
                    "Fields smallVal and len must be exclusive");
            }

            if (fldSmallVal != null) {
                if (!fldSmallVal.isMap()) {
                    throw RedisResponseException.corrupt(
                        "Invalid smallVal field");
                }
                smallVal = fldSmallVal.asMap();
            } else {
                len = fvToLen(fldLen);
            }
        }

        MapValue makeValue() {
            assert len > 0 ^ smallVal != null;
            MapValue res = super.makeValue().put(VALUE_TYPE, TYPE_HASH);
            
            if (len > 0) {
                res.put(FLD_LEN, len);
            } else {
                res.put(FLD_SMALL_VAL, smallVal);
            }
            
            return res;
        }

    }

    private class HashValueInfo {
        final HashHeader header;
        // existingIds and existingCnt are used exclusively (for HDEL and HSET
        // respectively).
        final ArrayList<String> existingIds;
        final int existingCnt;

        HashValueInfo(HashHeader header, int existingCnt) {
            this.header = header;
            this.existingCnt = existingCnt;
            existingIds = null;
        }

        HashValueInfo(HashHeader header, ArrayList<String> keyIds) {
            this.header = header;
            this.existingIds = keyIds;
            existingCnt = keyIds.size();
        }
    }

    private class HSetResult {
        final int addedCnt;
        final int processedCnt;

        HSetResult(int addedCnt, int processedCnt) {
            this.addedCnt = addedCnt;
            this.processedCnt = processedCnt;
        }
    }

    private static MapValue rowToSmallVal(MapValue row)
        throws RedisResponseException {
        HashHeader header = new HashHeader(rowToValue(row));
        if (header.smallVal == null) {
            throw RedisResponseException.corrupt("Missing smallVal");
        }
        return header.smallVal;
    }

    private static String getValFromSmallVal(MapValue smallVal, String keyId)
        throws RedisResponseException {
        FieldValue entry = smallVal.get(keyId);
        if (entry == null) {
            return null;
        }
        if (!entry.isMap()) {
            throw RedisResponseException.corrupt("Invalid entry in smallVal");
        }
        return getStringField(entry.asMap(), FLD_VALUE);
    }

    private static String rowToKeyId(MapValue row, boolean allowNull)
        throws RedisResponseException {
        return getStringField(row, FLD_KEY_ID, true);
    }

    private static String rowToFldVal(MapValue row, boolean allowNull)
        throws RedisResponseException {
        return getStringField(row, FLD_FLD_VAL, allowNull);
    }

    private CollectionValueResult<HashHeader> doGetHash(
        RedisKeyInfo keyInfo) throws RedisResponseException {
        RedisValueInfo valInfo = doGet(keyInfo);
        return new CollectionValueResult<HashHeader>(valInfo,
            valInfo.isValid() ? new HashHeader(valInfo.val) : null);
    }

    private void convertToMultiRow(RedisKeyInfo keyInfo,
        HashHeader header, CollectionUpdateInfo upInfo) {
        
        MapValue smallVal = header.smallVal;
        header.smallVal = null;
        header.len = smallVal.size();
        upInfo.addPutKeyReq(header);

        for(Map.Entry<String, FieldValue> ent : smallVal) {
            // The values in the map already contain key and value, we only
            // need to add id, keyId and cid.
            upInfo.addElemReq(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(ent.getValue().asMap()
                    .put(FLD_ID, keyInfo.id)
                    .put(FLD_KEY_ID, ent.getKey())
                    .put(FLD_CID, header.cid)));
        }
    }

    private CollectionValueResult<HashValueInfo> getExistingVal(
        RedisKeyInfo keyInfo, String[] fKeyIds, boolean toGetIds)
        throws RedisResponseException {
        // For HSET, we don't really need the actual field ids, only the
        // count of matching entries to find how many new fields are added.
        // However, currently there is no efficient way to retrieve the
        // count together with the parent row data in a single query.
        // For HDEL, it may be useful to have field ids so that we can create
        // delete requests only for existing fields (instead of all input
        // fields).
        List<MapValue> rows = doQuery(keyInfo,
            fKeyIds.length == 1 ? SQL_ENTRY_ID : SQL_ENTRY_IDS,
            fKeyIds.length == 1 ? new StringValue(fKeyIds[0]) :
                new ArrayValue().addAll(Arrays.stream(fKeyIds)
                    .map(val -> new StringValue(val))));

        if (rows.isEmpty()) {
            // Hash does not exist.
            return CollectionValueResult.none();
        }
    
        MapValue row0 = rows.get(0);
        RedisValueInfo val = new RedisValueInfo(rowToValue(row0),
            rowToVer(row0), getExpTime(rowToKey(row0)));
        HashHeader header = new HashHeader(val.val);
    
        String keyId0 = rowToKeyId(row0, true);
        ArrayList<String> existingIds = null;
        int existingCnt;

        if (keyId0 == null) {
            // No matching entries or hash stored in smallVal format.
            chkSingleResult(rows);
            existingCnt = 0;
        } else {
            if (header.smallVal != null) {
                throw RedisResponseException.corrupt(
                    "Invalid hash header in query result");
            }

            existingCnt = rows.size();

            if (toGetIds) {
                existingIds = new ArrayList<>(existingCnt);
                existingIds.add(keyId0);
                for(int i = 1; i < existingCnt; i++) {
                    existingIds.add(rowToKeyId(rows.get(i), false));
                }
            }
        }

        return new CollectionValueResult<>(val,
            existingIds == null ?
                new HashValueInfo(header, existingCnt) :
                new HashValueInfo(header, existingIds));
    }

    private HSetResult prepareHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        String[] fKeyIds, HashValueInfo hvi, CollectionUpdateInfo upInfo,
        boolean hasLargeEntry) throws RedisResponseException {
        HashHeader header = hvi != null ? hvi.header : null;
        boolean isNew = false;

        if (header == null) {
            header = new HashHeader();
            isNew = true;
        }

        int addedCnt;
        int processedCnt;

        if (header.smallVal != null) {
            addedCnt = 0;
            assert header.smallVal.size() < MAX_SMALL_HASH_SIZE;

            for(processedCnt = 0; processedCnt < fKeyIds.length;
                processedCnt++) {
                String keyId = fKeyIds[processedCnt];
                if (isNew || !header.smallVal.contains(keyId)) {
                    addedCnt++;
                }
                header.smallVal.put(keyId, fvMap.get(keyId));
                if (header.smallVal.size() == MAX_SMALL_HASH_SIZE) {
                    break;
                }
            }

            HSetResult res = new HSetResult(addedCnt, processedCnt);

            if (hasLargeEntry ||
                header.smallVal.size() == MAX_SMALL_HASH_SIZE) {
                convertToMultiRow(keyInfo, header, upInfo);
            } else {
                // The hash is still in smallVal format.
                upInfo.addPutKeyReq(header);
            }

            return res;
        }

        // The hash is in multi-row format. The value must have already
        // existed (otherwise we would be using smallVal above).
        assert hvi != null;

        if (header.len <= 0) {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }

        // addedCnt is number of fields set (no duplicates) minus number
        // of fields already existing (set in previous callback).
        processedCnt = fKeyIds.length;
        addedCnt = processedCnt - hvi.existingCnt;
        header.len += addedCnt;
        
        upInfo.addPutKeyReq(header);
        for(int i = 0; i < fKeyIds.length; i++) {
            String keyId = fKeyIds[i];
            MapValue val = fvMap.get(keyId).asMap();
            assert val != null;

            // The values in the map already contain key and value, we
            // only need to add id, keyId and cid.
            upInfo.addElemReq(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(val.put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, keyId)
                    .put(FLD_CID, header.cid)));
        }

        return new HSetResult(addedCnt, processedCnt);
    }

    private int doHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        boolean hasLargeEntry) throws RedisResponseException {
        String[] keyIds = fvMap.getMap().keySet().stream()
            .limit(MAX_TXN_ELEM_CNT).toArray(String[]::new);

        return doMultiUpdate(keyInfo,
            ki -> getExistingVal(ki, keyIds, false),
            (ki, hvi, upInfo) -> prepareHSet(ki, fvMap, keyIds, hvi, upInfo,
                hasLargeEntry),
            (hsr, wmRes) -> {
                // Remove elements we already processed. There may be fewer of
                // them than elements in keyIds (if we exceeded smallVal size
                // limit and called convertToMultiRow()).
                assert hsr.processedCnt > 0;
                for(int i = 0; i < hsr.processedCnt; i++) {
                    fvMap.remove(keyIds[i]);
                }
                return hsr.addedCnt;
            });
    }

    private int prepareHDel(RedisKeyInfo keyInfo, String[] fKeyIds,
        HashValueInfo hvi, CollectionUpdateInfo upInfo)
        throws RedisResponseException {
        if (hvi == null) {
            // Hash does not exist.
            return 0;
        }

        HashHeader header = hvi.header;
        assert header != null;

        if (header.smallVal != null) {
            int delCnt = 0;
            assert header.smallVal.size() < MAX_SMALL_HASH_SIZE;

            for(int i = 0; i < fKeyIds.length; i++) {
                if (header.smallVal.remove(fKeyIds[i]) != null) {
                    delCnt++;
                }
            }

            if (header.smallVal.size() != 0) {
                upInfo.addPutKeyReq(header);    
            } else {
                upInfo.addDeleteKeyReq();
            }

            return delCnt;
        }

        if (hvi.existingCnt == 0) {
            // Nothing to delete.
            return 0;
        }

        assert hvi.existingIds != null;
        assert hvi.existingIds.size() == hvi.existingCnt;
        header.len -= hvi.existingCnt;

        if (header.len > 0) {
            upInfo.addPutKeyReq(header);
        } else if (header.len == 0) {
            upInfo.addDeleteKeyReq();
        } else {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }
        
        // We only add delete requests for existing fields.
        for(int i = 0; i < hvi.existingCnt; i++) {
            upInfo.addElemReq(new DeleteRequest()
                .setTableName(HASH_TABLE_NAME)
                .setKey(new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_KEY_ID, hvi.existingIds.get(i))));
        }

        return hvi.existingCnt;
    }

    private int doHDel(RedisKeyInfo keyInfo, Set<String> fSet)
        throws RedisResponseException {
        String[] keyIds = fSet.stream().limit(MAX_TXN_ELEM_CNT)
            .toArray(String[]::new);

        return doMultiUpdate(keyInfo,
            ki -> getExistingVal(ki, keyIds, true),
            (ki, hvi, upInfo) -> prepareHDel(keyInfo, keyIds, hvi, upInfo),
            (delCnt, wmRes) -> {
                // Unlike for HSET, here all elements of keyIds should be
                // processed on successful request.
                for(int i = 0; i < keyIds.length; i++) {
                    fSet.remove(keyIds[i]);
                };
                return delCnt;
            });
    }


    protected String getElemsTblName() { return HASH_TABLE_NAME; }
    protected String getSQLSelElems() { return SQL_SEL_ELEMS; }
    protected String getSQLDelElems() { return SQL_DEL_ELEMS; }

    protected MapValue doCopy(RedisKeyInfo srcKeyInfo,
        RedisValueInfo srcValInfo, RedisKeyInfo dstKeyInfo)
        throws RedisResponseException {
        HashHeader header = new HashHeader(srcValInfo.val);
        if (header.smallVal != null) {
            // The hash is in smallVal format, no copy of elements needed.
            assert header.len == 0;
            // Using null as first arg will create new cid.
            return new HashHeader(null, header.smallVal, 0).makeValue();
        }

        // For multi-row format, we copy the elements and create the new
        // header.
        assert header.len > 0;
        CopyElemsResult copyRes = doCopyElems(srcKeyInfo, srcValInfo,
            dstKeyInfo);
        return new HashHeader(copyRes.cid, null, copyRes.cnt).makeValue();
    }

    public HashCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_HSET, this::handleHSet);
        cmdMap.put(CMD_HDEL, this::handleHDel);
        cmdMap.put(CMD_HLEN, this::handleHLen);
        cmdMap.put(CMD_HGET, this::handleHGet);
        cmdMap.put(CMD_HMGET, this::handleHMGet);
    }

    public RedisMessage handleHSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        // one or more field-value pairs plus the key
        if (cmd.args.length % 2 != 1) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        // We put the fields and values into a map at first even for multi-row
        // format. This is to account for potential duplicate fields in input.
        // Redis allows duplicate fields in SET commands and just uses the
        // last value to set. We have to elimitate duplicates to calculate
        // correct added count and also because WriteMultipleRequest cannot
        // contain requests with duplicate primary keys.
        MapValue fvMap = new MapValue();
        boolean hasLargeEntry = false;

        for(int i = 1; i < cmd.args.length; i += 2) {
            RedisKeyInfo hki = makeRedisKeyInfo(cmd.args[i]);
            String val = makeStrVal(cmd.args[i + 1]);
            
            // For simplicity, we store the data in the same format in
            // smallVal as in multi-row format, having "key" and "value"
            // field for each entry. Note that the key might also store
            // per-hash-field expiration time (added to Redis 7.4) as well
            // as scanId. It is preferable to compute scanId in the same
            // way as for multi-row format so that the scan still works
            // even if the hash gets converted to mutli-row format during
            // the scan.
            fvMap.put(hki.id, new MapValue()
                .put(FLD_KEY, makeRedisKey(hki)).put(FLD_VALUE, val));

            if (hki.data.length() + val.length() > MAX_SMALL_HASH_ENT_SIZE) {
                hasLargeEntry = true;
            }
        }

        int addedCnt = 0;
        while(fvMap.size() > 0) {
            addedCnt += doHSet(keyInfo, fvMap, hasLargeEntry);
        }

        return new IntegerRedisMessage(addedCnt);
    }

    public RedisMessage handleHDel(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        // We have to elimitate duplicates from the input for the same reason
        // as for HSET.
        HashSet<String> fSet = new HashSet<>();

        for(int i = 1; i < cmd.args.length; i++) {
            fSet.add(makeRedisKeyInfo(cmd.args[i]).id);
        }

        int delCnt = 0;
        while(fSet.size() > 0) {
            delCnt += doHDel(keyInfo, fSet);
        }

        return new IntegerRedisMessage(delCnt);
    }

    public RedisMessage handleHLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        HashHeader header = doGetHash(makeRedisKeyInfo(cmd.args[0])).data;
        long len = header != null ?
            (header.smallVal != null ? header.smallVal.size() : header.len) :
            0;
        return new IntegerRedisMessage(len);
    }

    public RedisMessage handleHGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        String keyId = makeRedisKeyInfo(cmd.args[1]).id;
        List<MapValue> rows = doQuery(makeRedisKeyInfo(cmd.args[0]),
            SQL_HGET_VAL, new StringValue(keyId));
        if (rows.size() > 1) {
            throw RedisResponseException.corrupt(
                "Expected at most a single result, got multiple");
        }

        if (rows.size() == 0) {
            // Hash does not exist or field is not found.
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        MapValue row0 = rows.get(0);
        String val = rowToFldVal(row0, true);
        if (val != null) {
            return new FullBulkStringRedisMessage(getStrVal(val));
        }

        // Hash must be in smallVal format. Note that for HGET and HMGET we
        // put the matching field condition in the WHERE clause (and not in
        // LOJ) since we don't need to distinguish between the cases where
        // hash does not exist or matching field(s) are not found (for HSET
        // we do need to distinguish these), so empty result is returned also
        // when there are no matching fields.
        val = getValFromSmallVal(rowToSmallVal(row0), keyId);
        return val != null ?
            new FullBulkStringRedisMessage(getStrVal(val)) :
            FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleHMGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        // We will reuse keyIds for convenience.
        ArrayValue keyIds = new ArrayValue().addAll(
            Arrays.stream(cmd.args, 1, cmd.args.length)
                .map(val -> new StringValue(makeRedisKeyInfo(val).id)));

        List<MapValue> rows = doQuery(makeRedisKeyInfo(cmd.args[0]),
            SQL_HGET_KEYVALS, keyIds);

        if (rows.isEmpty()) {
            // Hash does not exist or hash is in multi-row format and no
            // input fields found.
            return new ArrayRedisMessage(Collections.nCopies(keyIds.size(),
                FullBulkStringRedisMessage.NULL_INSTANCE));
        }

        // map and smallVal are exclusive
        HashMap<String, String> map = null;
        MapValue smallVal = null;

        MapValue row0 = rows.get(0);
        String keyId0 = rowToKeyId(row0, true);

        if (keyId0 == null) {
            chkSingleResult(rows);
            smallVal = rowToSmallVal(row0);
        } else {
            // The hash is in multi-row format. We collect the field values
            // into a map to retrieve them later when computing the result
            // (which has to return a value or nil for each input field in
            // the order of the input fields).
            int cnt = rows.size();
            map = new HashMap<>(cnt);
            map.put(keyId0, rowToFldVal(row0, false));
            for(int i = 0; i < cnt; i++) {
                map.put(rowToKeyId(row0, false),
                    rowToFldVal(row0, false));
            }
        }

        ArrayList<RedisMessage> res = new ArrayList<>();
        int cnt = keyIds.size();
        // For each field provided, we return the value if found, or null.
        for(int i = 0; i < cnt; i++) {
            String keyId = keyIds.get(i).getString();
            String val = smallVal != null ?
                getValFromSmallVal(smallVal, keyId) : map.get(keyId);
            res.add(val != null ?
                new FullBulkStringRedisMessage(getStrVal(val)) :
                FullBulkStringRedisMessage.NULL_INSTANCE);
        }

        return new ArrayRedisMessage(res);
    }
}
