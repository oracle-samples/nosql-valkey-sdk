package oracle.nosql.redis.commands;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;

public class HashCommandsBase extends CollectionCommandsBase {

    // We convert to multi-row format as soon as the size exceeds
    // MAX_SMALL_HASH_SIZE by 1. The value below is chosen such that
    // convertToMultirow() is done atomically. In future, this value may be
    // configurable (in which case it is possible to exceed the size by more
    // than 1 if config changes), but must never be greater than the one
    // specified below.

    //protected static final int MAX_SMALL_HASH_SIZE = MAX_TXN_ELEM_CNT - 2;
    //protected static final int MAX_SMALL_HASH_ENT_SIZE = 10 * 1024;
    protected static final int MAX_SMALL_HASH_SIZE = 5;
    protected static final int MAX_SMALL_HASH_ENT_SIZE = 10;

    protected static final String HASH_TABLE_NAME = "redis.hashes";
    protected static final String FLD_KEY_ID = "keyId";
    protected static final String FLD_SMALL_VAL = "smallVal";
    protected static final String FLD_FLD_KEY = "fldKey";

    protected static String VAR2_STRING = " $var2 STRING;";
    protected static String VAR2_STRING_ARRAY = " $var2 ARRAY(STRING);";
    protected static final String HKEYID_COND =
        " AND ($h.keyId IS NULL OR $h.keyId %s)";
    protected static String HKEYID_IN_ARRAY_VAR2 = String.format(HKEYID_COND,
        "IN $var2[]");
    protected static String HKEYID_EQ_VAL_VAR2 = String.format(HKEYID_COND,
        "= $var2");

    protected static final String FROM_LOJ =
        "FROM redis $r LEFT OUTER JOIN redis.hashes $h ON $r.id = $h.id " +
        "AND $r.value.cid = $h.cid";

    protected static final String SQL_ENTRY_IDS_FMT =
        "DECLARE $var1 STRING;%s SELECT row_version($r) AS ver, $r.key, " +
        "$r.value, $h.keyId " + FROM_LOJ + "%s WHERE $r.id = $var1 ";
    protected static final String SQL_ENTRY_IDS = String.format(
        SQL_ENTRY_IDS_FMT, VAR2_STRING_ARRAY, HKEYID_IN_ARRAY_VAR2);
    protected static final String SQL_ENTRY_ID = String.format(
            SQL_ENTRY_IDS_FMT, VAR2_STRING, HKEYID_EQ_VAL_VAR2);

    protected static final String SQL_SEL_ELEMS = String.format(
        SQL_SEL_ELEMS_FMT, HASH_TABLE_NAME);
    protected static final String SQL_DEL_ELEMS = String.format(
        SQL_DEL_ELEMS_FMT, HASH_TABLE_NAME);

    protected static final String ERR_INVALID_SMALLVAL_ENTRY =
        "Invalid entry in smallVal";
    
    public static final String CMD_HSET = "HSET";
    public static final String CMD_HDEL = "HDEL";
    public static final String CMD_HLEN = "HLEN";
    public static final String CMD_HGET = "HGET";
    public static final String CMD_HMGET = "HMGET";
    public static final String CMD_HSCAN = "HSCAN";
    public static final String CMD_HKEYS = "HKEYS";
    public static final String CMD_HVALS = "HVALS";
    public static final String CMD_HGETALL = "HGETALL";
    public static final String CMD_HEXISTS = "HEXISTS";
    public static final String CMD_HSTRLEN = "HSTRLEN";

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

    protected static class HashValueInfo {
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

    protected static MapValue valToSmallVal(MapValue val)
        throws RedisResponseException {
        HashHeader header = new HashHeader(val);
        if (header.smallVal == null) {
            throw RedisResponseException.corrupt("Missing smallVal");
        }
        return header.smallVal;
    }

    protected static String getValFromSmallVal(MapValue smallVal,
        String keyId) throws RedisResponseException {
        FieldValue entry = smallVal.get(keyId);
        if (entry == null) {
            return null;
        }
        if (!entry.isMap()) {
            throw RedisResponseException.corrupt(ERR_INVALID_SMALLVAL_ENTRY);
        }
        return getStringField(entry.asMap(), FLD_VALUE);
    }

    protected static String rowToKeyId(MapValue row, boolean allowNull)
        throws RedisResponseException {
        return getStringField(row, FLD_KEY_ID, true);
    }

    protected static String rowToFldVal(MapValue row, boolean allowNull)
        throws RedisResponseException {
        return getStringField(row, FLD_VALUE, allowNull);
    }

    protected CollectionValueResult<HashHeader> doGetHash(
        RedisKeyInfo keyInfo) throws RedisResponseException {
        RedisValueInfo valInfo = doGet(keyInfo);
        return new CollectionValueResult<HashHeader>(valInfo,
            valInfo.isValid() ? new HashHeader(valInfo.val) : null);
    }

    protected CollectionValueResult<HashValueInfo> getExistingVal(
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

    public HashCommandsBase(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

}
