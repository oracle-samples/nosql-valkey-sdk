/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import io.netty.buffer.ByteBuf;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;

import static oracle.nosql.redis.util.Utils.getStringField;

abstract class ListCommandsBase extends CollectionCommandsBase {

    protected static final String LIST_TABLE_NAME = "redis.lists";
    protected static final String FLD_ELEM_ID = "elemId";
    protected static final String FLD_ELEM_VAL = "elemVal";

    protected static final String LIMIT_1 = " LIMIT 1";
    protected static final String VAR2_LONG = "$var2 LONG; ";
    protected static final String VAR2_STR_VAR3_INT =
        "$var2 STRING; $var3 INTEGER; ";
    protected static final String LIMIT_VAR2 = " LIMIT $var2";
    protected static final String LIMIT_VAR3 = " LIMIT $var3";
    protected static final String OFFSET_VAR2 = " OFFSET $var2";
    protected static final String ELEM_VAL = ", $l.value AS elemVal";
    protected static final String ELEM_VAL_VAR2 = " AND $l.value = $var2 ";
    protected static final String VAL_LEN = ", $r.value.len AS len";
    protected static final String LIST_IDX_HINT =
        " /*+ FORCE_INDEX(redis.lists listPK2Idx) */ ";
    protected static final String FROM_JOIN_WHERE_KEY_ID =
        "FROM NESTED TABLES(redis.lists $l ANCESTORS(redis $r)) " +
            WHERE_KEY_ID_COND + "AND $l.cid = $r.value.cid ";
    protected static final String PK_COLS = "$l.slot, $l.id, $l.elemId";
    protected static final String PK_COLS_DESC =
        "$l.slot DESC, $l.id DESC, $l.elemId DESC";

    protected static final String SQL_SEL_ELEMS = String.format(
        SQL_SEL_ELEMS_FMT, LIST_TABLE_NAME);
    protected static final String SQL_DEL_ELEMS = String.format(
        SQL_DEL_ELEMS_FMT, LIST_TABLE_NAME);

    protected static final String SQL_ELEMS_FMT = DECL_KEY_ID + 
        "%sSELECT" + LIST_IDX_HINT +
        "row_version($r) AS ver, $r.key, $r.value, $l.elemId%s " +
        FROM_JOIN_WHERE_KEY_ID + "%sORDER BY %s%s%s";

    protected static final String SQL_LPUSH = String.format(SQL_ELEMS_FMT, "",
        "", "", PK_COLS, LIMIT_1, "");
    protected static final String SQL_RPUSH = String.format(SQL_ELEMS_FMT, "",
        "", "", PK_COLS_DESC, LIMIT_1, "");
    // Used for L/RPOP and LTRIM.
    protected static final String SQL_LPOP = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", PK_COLS, LIMIT_VAR2, "");
    protected static final String SQL_RPOP = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", PK_COLS_DESC, LIMIT_VAR2, "");

    // Queries element ids less/greater than given element id in order of
    // element id. Currently only used in ListSetInsert.java.
    protected static final String SQL_ELEM_ID_FMT = DECL_KEY_ID +
        "$var2 NUMBER; SELECT" + LIST_IDX_HINT + "$l.elemId " +
        FROM_JOIN_WHERE_KEY_ID + "AND $l.elemId %s $var2 ORDER BY %s%s";

    protected static final BigDecimal VALUE_TWO = new BigDecimal(2);

    public static final String CMD_LPUSH = "LPUSH";
    public static final String CMD_LPOP = "LPOP";
    public static final String CMD_RPUSH = "RPUSH";
    public static final String CMD_RPOP = "RPOP";
    public static final String CMD_LMPOP = "LMPOP";
    public static final String CMD_LPUSHX = "LPUSHX";
    public static final String CMD_RPUSHX = "RPUSHX";
	public static final String CMD_LLEN = "LLEN";
    public static final String CMD_LREM = "LREM";
    public static final String CMD_LINDEX = "LINDEX";
    public static final String CMD_LRANGE = "LRANGE";
    public static final String CMD_LSET = "LSET";
    public static final String CMD_LTRIM = "LTRIM";
    public static final String CMD_LPOS = "LPOS";
    public static final String CMD_LINSERT = "LINSERT";
    public static final String CMD_LMOVE = "LMOVE";
    public static final String CMD_RPOPLPUSH = "RPOPLPUSH";
    public static final String CMD_BLPOP = "BLPOP";
    public static final String CMD_BRPOP = "BRPOP";
    public static final String CMD_BLMPOP = "BLMPOP";
    public static final String CMD_BLMOVE = "BLMOVE";
    public static final String CMD_BRPOPLPUSH = "BRPOPLPUSH";

    protected static class ListHeader extends CollectionHeader {
        long len;

        ListHeader(String cid, long len) {
            super(cid);
            this.len = len;
        }

        ListHeader(long len) {
            this.len = len;
        }

        ListHeader(MapValue val) throws RedisResponseException {
            super(val);

            if (!getValueType(val).equals(TYPE_LIST)) {
                throw RedisResponseException.wrongType();
            }
            
            len = valToLen(val);
        }

        MapValue makeValue() {
            return super.makeValue().put(VALUE_TYPE, TYPE_LIST)
                .put(FLD_LEN, len);
        }
    }

    static class ListValueInfo {
        final ListHeader header;
        final List<BigDecimal> elemIds = new ArrayList<>();
        final List<String> elemVals;

        ListValueInfo(ListHeader header) {
            this.header = header;
            elemVals = null;
        }

        ListValueInfo(RedisValueInfo valInfo, List<MapValue> rows,
            boolean toGetElemVals) throws RedisResponseException {
            header = new ListHeader(valInfo.val);
            elemVals = toGetElemVals ? new ArrayList<>() : null;
            if (rows == null) {
                return;
            }

            // We should not have more elements that are in the list.
            if (rows.size() > header.len) {
                throw RedisResponseException.corrupt(
                    "Invalid list header or query result");
            }

            for(MapValue row : rows) {
                elemIds.add(rowToElemId(row));
                if (toGetElemVals) {
                    elemVals.add(rowToElemVal(row));
                }
            }
        }

        ListValueInfo(RedisValueInfo valInfo, List<MapValue> rows)
            throws RedisResponseException {
            this(valInfo, rows, false);
        }

        ListValueInfo(RedisValueInfo valInfo)
            throws RedisResponseException {
            this(valInfo, null, false);
        }

    }

    ListCommandsBase(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    protected static BigDecimal rowToElemId(MapValue row, boolean allowNull)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_ELEM_ID);
        if (allowNull && val != null && val.isAnyNull()) {
            return null;
        }
        if (val == null || !val.isNumber()) {
            throw RedisResponseException.corrupt("Invalid element id");
        }
        return val.getNumber();
    }

    protected static BigDecimal rowToElemId(MapValue row)
        throws RedisResponseException {
        return rowToElemId(row, false);
    }

    protected static String rowToElemVal(MapValue row)
        throws RedisResponseException {
        return getStringField(row, FLD_ELEM_VAL);
    }

    protected static long valToLen(MapValue val)
        throws RedisResponseException {
        return fvToLen(val.get(FLD_LEN));
    }

    protected static PutRequest makePutElemReq(RedisKeyInfo keyInfo,
        BigDecimal elemId, String cid, FieldValue val) {
        return new PutRequest().setTableName(LIST_TABLE_NAME)
            .setValue(new MapValue().put(FLD_SLOT, keyInfo.slot)
            .put(FLD_ID, keyInfo.id).put(FLD_ELEM_ID, elemId).put(FLD_CID, cid)
            .put(FLD_VALUE, val));
    }

    protected static PutRequest makePutElemReq(RedisKeyInfo keyInfo,
        BigDecimal elemId, String cid, ByteBuf val) {
        return makePutElemReq(keyInfo, elemId, cid,
            new StringValue(makeStrVal(val)));
    }

    protected static DeleteRequest makeDeleteElemReq(RedisKeyInfo keyInfo,
        BigDecimal elemId, boolean returnExisting) {
        return new DeleteRequest().setTableName(LIST_TABLE_NAME)
            .setKey(new MapValue().put(FLD_SLOT, keyInfo.slot)
            .put(FLD_ID, keyInfo.id).put(FLD_ELEM_ID, elemId))
            .setReturnRow(returnExisting);
    }

    protected static void addDeleteListElems(RedisKeyInfo keyInfo,
        ListValueInfo val, WriteMultipleRequest wmReq, RedisValueInfo oldVal,
        boolean returnExisting) throws RedisResponseException {
        assert(val != null);
        int delCnt = val.elemIds.size();
        assert delCnt != 0;

        ListHeader header = val.header;
        if (header.len < delCnt) {
            throw RedisResponseException.corrupt(
                "Invalid list element count");
        }

        if (header.len > delCnt) {
            // Can remove delCnt elements without emptying the list.
            header.len -= delCnt;
            wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
        } else { // header.len == delCnt
            // Remove all elements from the list and delete the list.
            wmReq.add(makeDeleteKeyReq(keyInfo, oldVal), true);
        }

        for(BigDecimal elemId : val.elemIds) {
            wmReq.add(makeDeleteElemReq(keyInfo, elemId, returnExisting),
                false);
        }
    }

    // We don't worry about expired list key here, since it will be handled
    // in doMultiUpdate().
    protected CollectionValueResult<ListValueInfo> queryListElems(
        RedisKeyInfo keyInfo, String sql, boolean allowNoElems,
        boolean toGetElemVals, FieldValue... vars)
        throws RedisResponseException {
        List<MapValue> rows = doQuery(keyInfo, sql, vars);

        if (rows.isEmpty()) {
            // If allowNoElems is true, it is possible that list does exist,
            // but the query has no matching results, in this case we have to
            // check for existence of list header. If allowNoElems is false,
            // we assume the list doesn't exist (otherwise it would imply
            // corrupted data).
            return allowNoElems ?
                doGetList(keyInfo) : CollectionValueResult.none();
        }

        MapValue row0 = rows.get(0);

        RedisValueInfo val = RedisValueInfo.create(rowToValue(row0),
            rowToVer(row0), getExpTime(rowToKey(row0)));
        ListValueInfo res = val.isValid() ?
            new ListValueInfo(val, rows, toGetElemVals) : null;
        return new CollectionValueResult<>(val, res);
    }

    protected CollectionValueResult<ListValueInfo> queryListElems(
        RedisKeyInfo keyInfo, String sql, boolean allowNoElems,
        FieldValue... vars)
        throws RedisResponseException {
        return queryListElems(keyInfo, sql, allowNoElems, false, vars);
    }

    protected CollectionValueResult<ListValueInfo> queryListElems(
        RedisKeyInfo keyInfo, String sql, FieldValue... vars)
        throws RedisResponseException {
        return queryListElems(keyInfo, sql, false, vars);
    }

    protected CollectionValueResult<ListValueInfo> doGetList(
        RedisKeyInfo keyInfo) throws RedisResponseException {
        RedisValueInfo val = doGet(keyInfo);
        
        if (!val.isValid()) {
            return CollectionValueResult.none();
        }

        return new CollectionValueResult<>(val, new ListValueInfo(val));
    }

    protected long doLLen(RedisKeyInfo keyInfo)
        throws RedisResponseException {
        ListValueInfo valInfo = doGetList(keyInfo).data;
        return valInfo != null ? valInfo.header.len : 0;
    }

    protected String getElemsTblName() { return LIST_TABLE_NAME; }
    protected String getSQLSelElems() { return SQL_SEL_ELEMS; }
    protected String getSQLDelElems() { return SQL_DEL_ELEMS; }

    protected MapValue doCopy(RedisKeyInfo srcKeyInfo,
        RedisValueInfo srcValInfo, RedisKeyInfo dstKeyInfo)
        throws RedisResponseException {
        CopyElemsResult copyRes = doCopyElems(srcKeyInfo, srcValInfo,
            dstKeyInfo);
        return new ListHeader(copyRes.cid, copyRes.cnt).makeValue();
    }

}
