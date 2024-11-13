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
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;

abstract class ListCommandsBase extends CollectionCommandsBase {

    protected static final String LIST_TABLE_NAME = "redis.lists";
    protected static final String FLD_ELEM_ID = "elemId";

    protected static final String DESC = " DESC";
    protected static final String LIMIT_1 = " LIMIT 1";
    protected static final String VAR2_LONG = "$var2 LONG; ";
    protected static final String VAR2_STR_VAR3_INT =
        "$var2 STRING; $var3 INTEGER; ";
    protected static final String LIMIT_VAR2 = " LIMIT $var2";
    protected static final String LIMIT_VAR3 = " LIMIT $var3";
    protected static final String OFFSET_VAR2 = " OFFSET $var2";
    // private static final String ELEM_VAL = ", $l.value AS elemVal";
    protected static final String ELEM_VAL_VAR2 = " AND $l.value = $var2";
    protected static final String VAL_LEN = ", $r.value.len AS len";

    protected static final String SQL_SEL_ELEMS = String.format(
        SQL_SEL_ELEMS_FMT, LIST_TABLE_NAME);
    protected static final String SQL_DEL_ELEMS = String.format(
        SQL_DEL_ELEMS_FMT, LIST_TABLE_NAME);

    // The commented line below currently not working for DESC order 
    // because of a bug, so we use this variation for now.
    protected static final String SQL_ELEMS_FMT =
        "DECLARE $var1 STRING; %sSELECT row_version($r) AS ver, $r.key, " +
        "$r.value, $l.elemId%s FROM redis $r LEFT OUTER JOIN redis.lists $l " +
        //"ON $r.id = $l.id WHERE $r.id = $var1%s AND $l.cid = $r.value.cid " +
        "ON $r.id = $l.id AND $r.id = $var1 WHERE $l.cid = $r.value.cid%s " +
        "ORDER BY $l.id%s, $l.elemId%s%s%s";
    
    protected static final String SQL_LPUSH = String.format(SQL_ELEMS_FMT, "",
        "", "", "", "", LIMIT_1, "");
    protected static final String SQL_RPUSH = String.format(SQL_ELEMS_FMT, "",
        "", "", DESC, DESC, LIMIT_1, "");
    protected static final String SQL_LPOP = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", "", "", LIMIT_VAR2, "");
    protected static final String SQL_RPOP = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", DESC, DESC, LIMIT_VAR2, "");

    // Queries element ids less/greater than given element id in order of
    // element id. Currently only used in ListSetInsert.java.
    protected static final String SQL_ELEM_ID_FMT =
        "DECLARE $var1 STRING; $var2 NUMBER; SELECT $l.elemId FROM redis $r " +
        "LEFT OUTER JOIN redis.lists $l " +
        // "ON $r.id = $l.id WHERE $r.id = $var1 AND $l.cid = $r.value.cid " +
        "ON $r.id = $l.id AND $r.id = $var1 WHERE $l.cid = $r.value.cid " +
        "AND $l.elemId %s $var2 ORDER BY $l.id%s, $l.elemId%s%s";

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
    public static final String CMD_BLPOP = "BLPOP";
    public static final String CMD_BRPOP = "BRPOP";
    public static final String CMD_BLMPOP = "BLMPOP";

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
        final ArrayList<BigDecimal> elemIds = new ArrayList<>();

        ListValueInfo(ListHeader header) {
            this.header = header;
        }

        ListValueInfo(RedisValueInfo valInfo) throws RedisResponseException {
            this(new ListHeader(valInfo.val));
        }
    }

    ListCommandsBase(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
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
        return getStringField(row, FLD_VALUE);
    }

    protected static long valToLen(MapValue val)
        throws RedisResponseException {
        return fvToLen(val.get(FLD_LEN));
    }

    protected static PutRequest makePutElemReq(RedisKeyInfo keyInfo,
        BigDecimal elemId, String cid, FieldValue val) {
        return new PutRequest().setTableName(LIST_TABLE_NAME)
            .setValue(new MapValue().put(FLD_ID, keyInfo.id)
            .put(FLD_ELEM_ID, elemId).put(FLD_CID, cid)
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
            .setKey(new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_ELEM_ID, elemId))
            .setReturnRow(returnExisting);
    }

    protected static ListHeader makeDeleteListElems(RedisKeyInfo keyInfo,
        ListValueInfo val, CollectionUpdateInfo upInfo,
        boolean returnExisting) throws RedisResponseException {
        
        if (val == null) {
            return null;
        }

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
            upInfo.addPutKeyReq(header);
        } else { // header.len == delCnt
            // Remove all elements from the list and delete the list.
            upInfo.addDeleteKeyReq();
        }

        for(BigDecimal elemId : val.elemIds) {
            upInfo.addElemReq(makeDeleteElemReq(keyInfo, elemId,
                returnExisting));
        }

        return header;
    }

    // We don't worry about expired list key here, since it will be handled
    // in doMultiUpdate().
    protected CollectionValueResult<ListValueInfo> queryListElems(
        RedisKeyInfo keyInfo, String sql, boolean allowNoElems,
        FieldValue... vars) throws RedisResponseException {
        List<MapValue> rows = doQuery(keyInfo, sql, vars);

        // Note that the situation where we get no list elements matching list
        // key or cid of the list while still having valid list header, is not
        // possible, it would imply corrupted data (the elements with
        // different cid are possible after list deletion before these
        // elements are cleaned up, or if the list is expired and new one is
        // created, but in these cases the list header would be already
        // deleted and/or replaced with another valid list).
        if (rows.isEmpty()) {
            return CollectionValueResult.none(); // list does not exist
        }
        
        MapValue row0 = rows.get(0);

        RedisValueInfo val = new RedisValueInfo(rowToValue(row0),
            rowToVer(row0), getExpTime(rowToKey(row0)));
        ListValueInfo res = new ListValueInfo(val);
        
        BigDecimal elemId0 = rowToElemId(row0, allowNoElems);
        if (allowNoElems && elemId0 == null) {
            chkSingleResult(rows);
            return new CollectionValueResult<>(val, res);
        }

        assert elemId0 != null;
        res.elemIds.add(elemId0);
        int cnt = rows.size();

        // The query should not return more elements that are in the list.
        if (cnt > res.header.len) {
            throw RedisResponseException.corrupt(
                "Invalid list header or query result");
        }

        for(int i = 1; i < cnt; i++) {
            res.elemIds.add(rowToElemId(rows.get(i)));
        }

        return new CollectionValueResult<>(val, res);
    }

    protected CollectionValueResult<ListValueInfo> queryListElems(
        RedisKeyInfo keyInfo, String sql, FieldValue... vars)
        throws RedisResponseException {
        return queryListElems(keyInfo, sql, false, vars);
    }

    protected CollectionValueResult<ListValueInfo> doGetList(
        RedisKeyInfo keyInfo)throws RedisResponseException {
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
