/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;

import static oracle.nosql.redis.util.Utils.getStringField;

public class HashCommandsBase extends CollectionCommandsBase {

    // For the empty record key, we want to have max possible value for scanId
    // so that our scan query (see HashScan in HashRead.java) always fetches
    // it based on ">=" predicate it uses. This also ensures that the empty
    // record will be the last result (the results are ordered by scanId).
    private static final MapValue EMPTY_REC_KEY =
        new MapValue(1).put(KEY_SCAN_ID, Long.MAX_VALUE);

    // We convert to multi-row format as soon as the size exceeds
    // MAX_SMALL_HASH_SIZE by 1. The value below is chosen such that
    // convertToMultirow() is done atomically. In the future, this value may be
    // configurable (in which case it is possible to exceed the size by more
    // than 1 if config changes), but must never be greater than the one
    // specified below.

    protected static final int MAX_SMALL_HASH_SIZE = MAX_TXN_ELEM_CNT - 2;
    protected static final int MAX_SMALL_HASH_ENT_SIZE = 10 * 1024;
    //protected static final int MAX_SMALL_HASH_SIZE = 5;
    //protected static final int MAX_SMALL_HASH_ENT_SIZE = 10;

    protected static final String HASH_TABLE_NAME = "valkey.hashes";
    protected static final String FLD_KEY_ID = "keyId";
    protected static final String FLD_SMALL_VAL = "smallVal";
    protected static final String FLD_FLD_KEY = "fldKey";
    protected static final String FLD_FLD_VAL = "fldVal";

    protected static String VAR2_STRING = " $var2 STRING;";
    protected static String VAR2_STRING_ARRAY = " $var2 ARRAY(STRING);";
    protected static final String HKEYID_COND = " AND $h.keyId ";
    protected static final String SEL_HKEYID = ", $h.keyId";
    protected static final String SEL_FLDVAL = ", $h.value AS fldVal";

    // Currently, a SQL join of tables redis to valkey.hashes will not use
    // child table primary index to perform the hKeyId look up and thus will
    // not be O(1), which is not adequate for our purposes. Instead, we
    // reverse the join (valkey.hashes to redis) to allow using the child table
    // primary key index. However, because we store small hashes inline as
    // smallVal, we need to get a row from redis when there are no rows in
    // valkey.hashes. With this join, this is impossible, so instead we store an
    // "empty" record (in addition to any other records for this hash). We
    // use its hKeyId as empty string (which should not clash with valid
    // hKeyIds) so that it is always in the lowest sorted order.
    // To account for the empty record, we have to modify the queries
    // accordingly.
    // Update: for performance reasons, not using ORDER BY in the query
    // anymore, so we will not rely on ordering of returned rows and the
    // relative position of the empty record.
    protected static String HKEYID_IN_ARRAY_VAR2 =
        HKEYID_COND + "IN seq_concat('', $var2[])";
    protected static String HKEYID_EQ_VAL_VAR2 =
        HKEYID_COND + "IN ('', $var2)";

    protected static final String H_PK_COND =
        "$h.slot = $slot AND $h.id = $id ";
    protected static final String WHERE_H_PK_COND = SQL_WHERE + H_PK_COND;
    protected static final String FROM_JOIN_WHERE_H_PK =
        "FROM NESTED TABLES(valkey.hashes $h ANCESTORS(redis $r)) " +
            WHERE_H_PK_COND + "AND $h.cid = $r.value.cid ";

    protected static final String SQL_ENTRIES_FMT = DECL_KEY_ID +
        "%s SELECT row_version($r) AS ver, $r.key, $r.value%s " +
        FROM_JOIN_WHERE_H_PK + "%s";
    protected static final String SQL_ENTRY_IDS = String.format(
        SQL_ENTRIES_FMT, VAR2_STRING_ARRAY, SEL_HKEYID, HKEYID_IN_ARRAY_VAR2);
    protected static final String SQL_ENTRY_ID = String.format(
        SQL_ENTRIES_FMT, VAR2_STRING, SEL_HKEYID, HKEYID_EQ_VAL_VAR2);
    protected static final String SQL_ENTRY_VAL = String.format(
        SQL_ENTRIES_FMT, VAR2_STRING, SEL_FLDVAL, HKEYID_EQ_VAL_VAR2);

    protected static final String SQL_SEL_ELEMS = String.format(
        SQL_SEL_ELEMS_FMT, HASH_TABLE_NAME);
    protected static final String SQL_DEL_ELEMS = String.format(
        SQL_DEL_ELEMS_FMT, HASH_TABLE_NAME);

    protected static final String ERR_INVALID_HASH_ENTRY =
        "Invalid hash entry";
    protected static final String ERR_INVALID_HASH_HEADER =
        "Invalid hash header in query result";

    protected static final int MAX_ENTRY_LEN = 256 * 1024;
    
    public static final String CMD_HSET = "HSET";
    public static final String CMD_HMSET = "HMSET";
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
    public static final String CMD_HINCRBY = "HINCRBY";
    public static final String CMD_HINCRBYFLOAT = "HINCRBYFLOAT";
    public static final String CMD_HSETNX = "HSETNX";

    // Because small hashes are used often, we use an optimization: before
    // a hash reaches certain size, we store it as single MapValue inside the
    // "smallVal" subfield of "value" field in the main redis table. Once
    // has reaches certain size, we convert it to multi-row format where each
    // entry is stored in a separate row in "valkey.hashes" table and the
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

    protected static class HSetInfo {
        final HashHeader header;
        // This need to be changed to a list of RedisKeyInfo when we add
        // per-entry expiration.
        final int existingCnt;

        HSetInfo(HashHeader header, int existingCnt) {
            assert header != null;
            this.header = header;
            this.existingCnt = existingCnt;
        }
    }

    protected static class HDelInfo {
        final HashHeader header;
        final List<String> existingIds;

        HDelInfo(HashHeader header, List<String> keyIds) {
            assert header != null;
            this.header = header;
            this.existingIds = keyIds;
        }
    }

    protected static class HValInfo {
        final HashHeader header;
        final String fldVal;

        HValInfo(HashHeader header, String fldVal)
            throws RedisResponseException {
            this.header = header;
            this.fldVal = fldVal;
        }
    }

    protected static MapValue valToSmallVal(MapValue val, boolean allowNull)
        throws RedisResponseException {
        HashHeader header = new HashHeader(val);
        if (header.smallVal == null && !allowNull) {
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
            throw RedisResponseException.corrupt(ERR_INVALID_HASH_ENTRY);
        }
        return getStringField(entry.asMap(), FLD_VALUE);
    }

    protected static String rowToKeyId(MapValue row)
        throws RedisResponseException {
        return getStringField(row, FLD_KEY_ID);
    }

    protected static String rowToFldVal(MapValue row)
        throws RedisResponseException {
        return getStringField(row, FLD_VALUE);
    }

    protected static PutRequest makePutEmptyReq(RedisKeyInfo keyInfo,
        HashHeader header) {
        return new PutRequest()
            .setTableName(HASH_TABLE_NAME)
            .setValue(new MapValue().put(FLD_SLOT, keyInfo.slot)
                .put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, "")
                .put(FLD_CID, header.cid)
                .put(FLD_KEY, EMPTY_REC_KEY));
    }

    protected static DeleteRequest makeDeleteEmptyReq(RedisKeyInfo keyInfo) {
        return new DeleteRequest()
            .setTableName(HASH_TABLE_NAME)
            .setKey(new MapValue().put(FLD_SLOT, keyInfo.slot)
                .put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, ""));
    }

    // Static method is needed for HashScan.
    // Checks if the hash exists at given key, if so, verifies it is a hash.
    // Unfortunately, even keeping empty record in valkey.hashes table will not
    // guard against the case the key is of the wrong type (not hash), since
    // SQL JOIN will return no records in this case. For read-only commands,
    // we have no choice but to do this check if the query returns no records,
    // so that we return correct return value or error to the user. For update
    // commands, we can optimize this somewhat and do this check only in case
    // of a retry (see doChkWM in HashUpdate.java).
    protected static boolean chkHashKey(CommandsBase cmds,
        RedisKeyInfo keyInfo) throws RedisResponseException {
        RedisValueInfo valInfo = cmds.doGet(keyInfo);
        if (!valInfo.isValid()) {
            return false;
        }
        if (!getValueType(valInfo.val).equals(TYPE_HASH)) {
            throw RedisResponseException.wrongType();
        }
        return true;
    }

    protected boolean chkHashKey(RedisKeyInfo keyInfo)
        throws RedisResponseException {
        return chkHashKey(this, keyInfo);
    }

    protected CollectionValueResult<HashHeader> doGetHash(
        RedisKeyInfo keyInfo) throws RedisResponseException {
        RedisValueInfo valInfo = doGet(keyInfo);
        return new CollectionValueResult<HashHeader>(valInfo,
            valInfo.isValid() ? new HashHeader(valInfo.val) : null);
    }

    protected <T> CollectionValueResult<T> getExistingVal(
        RedisKeyInfo keyInfo, String[] fKeyIds,
        ThrowingBiFunction<HashHeader, List<MapValue>, T,
        RedisResponseException> getValInfo)
        throws RedisResponseException {
        // For HSET, we don't really need the actual field ids, only the
        // count of matching entries to find how many new fields are added.
        // However, currently there is no efficient way to retrieve the
        // count together with the parent row data in a single query (if using
        // GROUP BY, we would have to include all needed parent table columns
        // as grouping expressions, doing such grouping would be less
        // efficient than just returning the ids).
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

        RedisValueInfo val = RedisValueInfo.create(rowToValue(row0),
            rowToVer(row0), getExpTime(rowToKey(row0)));
        if (!val.isValid()) {
            // Hash expired.
            return new CollectionValueResult<>(val, null);
        }

        HashHeader header = new HashHeader(val.val);

        T res;
        if (rows.size() == 1) {
            // Hash is in smallVal format or none of the fields are found.
            // In either case, the only returned row should be the empty
            // record.
            if (!rowToKeyId(row0).isEmpty()) {
                throw RedisResponseException.corrupt(ERR_INVALID_HASH_ENTRY);
            }
            res = getValInfo.apply(header, null);
        } else {
            // Hash is in multi-row format and at least some fields are found.
            chkMaxNumResults(rows, fKeyIds.length + 1);
            if (header.smallVal != null) {
                throw RedisResponseException.corrupt(ERR_INVALID_HASH_HEADER);
            }
            res = getValInfo.apply(header, rows.stream().filter(
                Utils.uncheckedPred(row -> !rowToKeyId(row).isEmpty()))
                .collect(Collectors.toList()));
        }

        return new CollectionValueResult<>(val, res);
    }

    protected String getElemsTblName() { return HASH_TABLE_NAME; }
    protected String getSQLSelElems() { return SQL_SEL_ELEMS; }
    protected String getSQLDelElems() { return SQL_DEL_ELEMS; }

    protected MapValue doCopy(RedisKeyInfo srcKeyInfo,
        RedisValueInfo srcValInfo, RedisKeyInfo dstKeyInfo)
        throws RedisResponseException {
        HashHeader header = new HashHeader(srcValInfo.val);
        if (header.smallVal != null) {
            // The hash is in smallVal format, no copy of elements needed, but
            // we still need to create the empty record at destination.
            assert header.len == 0;
            header.cid = UUID.randomUUID().toString(); // assign new cid
            nosqlHandle.put(makePutEmptyReq(dstKeyInfo, header));
        } else {
            // For multi-row format, we copy the elements and return new
            // header for destination.
            assert header.len > 0;
            CopyElemsResult copyRes = doCopyElems(srcKeyInfo, srcValInfo,
                dstKeyInfo);

            header.cid = copyRes.cid;
            header.len = copyRes.cnt;
        }

        return header.makeValue();
    }

    public HashCommandsBase(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

}
