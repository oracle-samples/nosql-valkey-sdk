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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class ListCommands extends CollectionCommandsBase {

    private static final String LIST_TABLE_NAME = "redis.lists";
    private static final String FLD_LEN = "len";
    private static final String FLD_ELEM_ID = "elemId";

    private static final String DESC = " DESC";
    private static final String LIMIT_1 = " LIMIT 1";
    private static final String VAR2_LONG = "$var2 LONG; ";
    private static final String VAR2_STR_VAR3_INT =
        "$var2 STRING; $var3 INTEGER; ";
    private static final String LIMIT_VAR2 = " LIMIT $var2";
    private static final String LIMIT_VAR3 = " LIMIT $var3";
    private static final String OFFSET_VAR2 = " OFFSET $var2";
    // private static final String ELEM_VAL = ", $l.value AS elemVal";
    private static final String ELEM_VAL_VAR2 = " AND $l.value = $var2";
    private static final String NOT_EXPIRED =
        "AND (NOT EXISTS $r.key.exp OR $r.key.exp > current_time_millis()) ";
    private static final String VAL_LEN = ", $r.value.len AS len";

    // The commented line below currently not working for DESC order 
    // because of a bug, so we use this variation for now.
    private static final String SQL_ELEMS_FMT =
        "DECLARE $var1 STRING; %sSELECT row_version($r) AS ver, $r.key, " +
        "$r.value, $l.elemId%s FROM redis $r LEFT OUTER JOIN redis.lists $l " +
        //"ON $r.id = $l.id WHERE $r.id = $var1%s AND $l.cid = $r.value.cid " +
        "ON $r.id = $l.id AND $r.id = $var1 WHERE $l.cid = $r.value.cid%s " +
        "ORDER BY $l.id%s, $l.elemId%s%s%s";

    private static final String SQL_LPUSH = String.format(SQL_ELEMS_FMT, "",
        "", "", "", "", LIMIT_1, "");
    private static final String SQL_RPUSH = String.format(SQL_ELEMS_FMT, "",
        "", "", DESC, DESC, LIMIT_1, "");
    private static final String SQL_LPOP = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", "", "", LIMIT_VAR2, "");
    private static final String SQL_RPOP = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", DESC, DESC, LIMIT_VAR2, "");

    // Commented out line below for the same reason as in SQL_ELEMS_FMT
    private static final String SQL_LRANGE_FMT =
        "DECLARE $var1 STRING; $var2 INTEGER; $var3 LONG; SELECT $l.value " +
        "from redis $r LEFT OUTER JOIN redis.lists $l " +
        // "ON $r.id = $l.id WHERE $r.id = $var1 AND $l.cid = $r.value.cid " +
        "ON $r.id = $l.id AND $r.id = $var1 WHERE $l.cid = $r.value.cid " +
        NOT_EXPIRED +
        "ORDER BY $l.id%s, $l.elemId%s LIMIT $var2 OFFSET $var3";

    private static final String SQL_LRANGE = String.format(SQL_LRANGE_FMT,
        "", "");
    private static final String SQL_LRANGE_DESC = String.format(
        SQL_LRANGE_FMT, DESC, DESC);

    // Commented out line below for the same reason as in SQL_ELEMS_FMT
    private static final String SQL_LPOS_FMT =
        "DECLARE $var1 STRING; %sSELECT $l.value%s from redis $r " +
        "LEFT OUTER JOIN redis.lists $l " +
        // "ON $r.id = $l.id WHERE $r.id = $var1 AND $l.cid = $r.value.cid " +
        "ON $r.id = $l.id AND $r.id = $var1 WHERE $l.cid = $r.value.cid " +
        NOT_EXPIRED + "ORDER BY $l.id%s, $l.elemId%s%s";
    private static final String SQL_LPOS = String.format(SQL_LPOS_FMT, "", "",
        "", "", "");
    private static final String SQL_LPOS_MAXLEN = String.format(SQL_LPOS_FMT,
        VAR2_LONG, "", "", "", LIMIT_VAR2);
    private static final String SQL_LPOS_DESC = String.format(SQL_LPOS_FMT,
        "", VAL_LEN, DESC, DESC, "");
    private static final String SQL_LPOS_DESC_MAXLEN = String.format(
        SQL_LPOS_FMT, VAR2_LONG, VAL_LEN, DESC, DESC, LIMIT_VAR2);

    private static final String SQL_LSET = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", "", "", LIMIT_1, OFFSET_VAR2);
    private static final String SQL_LSET_DESC = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", DESC, DESC, LIMIT_1, OFFSET_VAR2);

    private static final String SQL_LREM = String.format(SQL_ELEMS_FMT,
        VAR2_STR_VAR3_INT, "", ELEM_VAL_VAR2, "", "", LIMIT_VAR3, "");
    private static final String SQL_LREM_DESC = String.format(SQL_ELEMS_FMT,
        VAR2_STR_VAR3_INT, "", ELEM_VAL_VAR2, DESC, DESC, LIMIT_VAR3, "");

    // The query to find pivot elemId for LINSERT command.
    // Note that per return value spec, we are required to differentiate
    // between the case when list key does not exist and the case when list
    // exists but the pivot is not found. 
    private static final String SQL_LINSERT_PIVOT =
        "DECLARE $var1 STRING; $var2 STRING; SELECT row_version($r) AS ver, " +
        "$r.key, $r.value, $l.elemId FROM redis $r LEFT OUTER JOIN " +
        "redis.lists $l ON $r.id = $l.id " + ELEM_VAL_VAR2 +
        " WHERE $r.id = $var1 AND $l.cid = $r.value.cid ORDER BY " +
        "$l.id, $l.elemId" + LIMIT_1;
    // Selects 1 element either before or after the pivot (given by its
    // elemId). Unfortunately, since subquery is not supported, we cannot find
    // pivot and the id before/after in a single query.
    private static final String SQL_LINSERT_ELEM_FMT =
        "DECLARE $var1 STRING; $var2 NUMBER; SELECT $l.elemId FROM redis $r " +
        "LEFT OUTER JOIN redis.lists $l " +
        // "ON $r.id = $l.id WHERE $r.id = $var1 AND $l.cid = $r.value.cid " +
        "ON $r.id = $l.id AND $r.id = $var1 WHERE $l.cid = $r.value.cid " +
        "AND $l.elemId %s $var2 ORDER BY $l.id%s, $l.elemId%s " + LIMIT_1;
    private static final String SQL_LINSERT_AFTER = String.format(
        SQL_LINSERT_ELEM_FMT, ">", "", "");
    private static final String SQL_LINSERT_BEFORE = String.format(
        SQL_LINSERT_ELEM_FMT, "<", DESC, DESC);
    
    private static final String SQL_SEL_ELEMS =
        "DECLARE $var1 STRING; $var2 STRING; SELECT * FROM redis.lists " +
        "WHERE id = $var1 AND cid = $var2";
    private static final String SQL_DEL_ELEMS =
        "DECLARE $var1 STRING; $var2 STRING; DELETE FROM redis.lists WHERE " +
        "id = $var1 AND cid = $var2";

    private static final BigDecimal VALUE_TWO = new BigDecimal(2);

    private static final int ELEM_ID_MAX_SCALE = 20;
    private static final int ELEM_ID_PREF_MAX_SCALE = 10;

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

    private static class ListHeader extends CollectionHeader {
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

    private static class ListTrimInfo extends ListHeader {
        long start;
        long stop;

        ListTrimInfo(long len, long start, long stop) {
            super(len);
            this.start = start;
            this.stop = stop;
        }
    }

    private static class LPosState {
        private final String elemVal;
        private final ArrayList<Long> matches;
        private long rank;
        private long len = -1;
        private long pos;
        private final int count;
        private final boolean isDesc;

        LPosState(ByteBuf elemVal, long rank, int count, boolean isDesc) {
            assert elemVal != null;
            this.elemVal = makeStrVal(elemVal);
            this.count = count;
            matches = count != 0 ?
                new ArrayList<>((int)count) : new ArrayList<>();
            this.isDesc = isDesc;
            this.rank = rank;
        }

        // Returns true if we have count matches, otherwise false.
        boolean applyRow(MapValue row) throws RedisResponseException {
            if (isDesc && len == -1) {
                len = valToLen(row);
            }

            if (elemVal.equals(rowToElemVal(row))) {
                if (rank == 1) {
                    matches.add(pos);
                } else {
                    rank--;
                }
                if (matches.size() == count) {
                    return true;
                }
            }

            pos++;
            return false;
        }

        ArrayList<Long> getMatches() {
            return matches;
        }

        long getLen() {
            return len;
        }
        
    }

    public ListCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private static BigDecimal rowToElemId(MapValue row, boolean allowNull)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_ELEM_ID);
        if (val == null || !val.isNumber()) {
            if (allowNull && val.isAnyNull()) {
                return null;
            }
            throw RedisResponseException.corrupt("Invalid element id");
        }
        return val.getNumber();
    }

    private static BigDecimal rowToElemId(MapValue row)
        throws RedisResponseException {
        return rowToElemId(row, false);
    }

    private static String rowToElemVal(MapValue row)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_VALUE);
        if (val == null || !val.isString()) {
            throw RedisResponseException.corrupt(
                "Invalid list element value");
        }
        return val.getString();
    }

    private static long valToLen(MapValue val) throws RedisResponseException {
        FieldValue fldLen = val.get(FLD_LEN);
        if (fldLen == null || !fldLen.isLong()) {
            throw RedisResponseException.corrupt(
                "Missing or invalid len field");
        }

        long len = fldLen.getLong();
        if (len <= 0) {
            throw RedisResponseException.corrupt(
                "Invalid list length: " + len);
        }
        return len;
    }

    private static PutRequest makePutElemReq(RedisKeyInfo keyInfo,
        BigDecimal elemId, String cid, ByteBuf val) {
        return new PutRequest().setTableName(LIST_TABLE_NAME)
            .setValue(new MapValue().put(FLD_ID, keyInfo.id)
            .put(FLD_ELEM_ID, elemId).put(FLD_CID, cid)
            .put(FLD_VALUE, makeStrVal(val)));
    }

    private static DeleteRequest makeDeleteElemReq(RedisKeyInfo keyInfo,
        BigDecimal elemId, boolean returnExisting) {
        return new DeleteRequest().setTableName(LIST_TABLE_NAME)
            .setKey(new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_ELEM_ID, elemId))
            .setReturnRow(returnExisting);
    }

    private static <T extends ListHeader> CollectionUpdateInfo<T>
        makeDeleteListElems(RedisKeyInfo keyInfo,
        CollectionValueInfo<T, BigDecimal> oldVal,
        boolean returnExisting) throws RedisResponseException {
        
        T header = oldVal.val;
        if (header == null) {
            return null;
        }

        int delCnt = oldVal.elems.size();
        assert delCnt != 0;

        if (header.len < delCnt) {
            throw RedisResponseException.corrupt(
                "Invalid list element count");
        }

        CollectionUpdateInfo<T> opInfo;
        if (header.len > delCnt) {
            // Can remove delCnt elements without emptying the list.
            header.len -= delCnt;
            opInfo = new CollectionUpdateInfo<>(header);
            opInfo.addPutKeyReq(keyInfo, oldVal);
        } else { // header.len == delCnt
            // Remove all elements from the list and delete the list.
            opInfo = new CollectionUpdateInfo<T>(null);
            opInfo.addDeleteKeyReq(keyInfo, oldVal);
        }

        for(BigDecimal elemId : oldVal.elems) {
            opInfo.addElemReq(makeDeleteElemReq(keyInfo, elemId,
                returnExisting));
        }

        return opInfo;
    }

    // We don't worry about expired list key here, since it will be handled
    // in doMultiUpdate().
    private CollectionValueInfo<ListHeader, BigDecimal> queryListElems(
        RedisKeyInfo keyInfo, String sql, boolean allowNoElems,
        FieldValue... vars)
        throws RedisResponseException {
        List<MapValue> rows = doQuery(keyInfo, sql, vars);

        // Note that the situation where we get no list elements matching list
        // key or cid of the list while still having valid list header, is not
        // possible, it would imply corrupted data (the elements with
        // different cid are possible after list deletion before these
        // elements are cleaned up, or if the list is expired and new one is
        // created, but in these cases the list header would be already
        // deleted and/or replaced with another valid list).
        if (rows.isEmpty()) {
            return CollectionValueInfo.none(); // list does not exist
        }
        
        MapValue row0 = rows.get(0);
        CollectionValueInfo<ListHeader, BigDecimal> res =
            new CollectionValueInfo<>(new ListHeader(rowToValue(row0)),
            oracle.nosql.driver.Version.createVersion(rowToVer(row0)),
            getExpTime(rowToKey(row0)));
        
        BigDecimal elemId0 = rowToElemId(row0, allowNoElems);
        if (allowNoElems && elemId0 == null) {
            chkSingleResult(rows);
            return res;
        }

        assert elemId0 != null;
        res.elems.add(elemId0);

        for(int i = 1; i < rows.size(); i++) {
            res.elems.add(rowToElemId(rows.get(i)));
        }

        return res;
    }

    private CollectionValueInfo<ListHeader, BigDecimal> queryListElems(
        RedisKeyInfo keyInfo, String sql, FieldValue... vars)
        throws RedisResponseException {
        return queryListElems(keyInfo, sql, false, vars);
    }

    private ListHeader doLRPush(RedisKeyInfo keyInfo, ByteBuf[] elems,
        int off, int cnt, boolean isLeft, boolean ifExists)
        throws RedisResponseException {
        assert cnt > 0;
        return doMultiUpdate(keyInfo, (ki) -> queryListElems(ki,
                isLeft ? SQL_LPUSH : SQL_RPUSH),
            (ki, oldVal) -> {
                ListHeader header = oldVal.val;

                BigDecimal startId;
                if (header != null) {
                    assert oldVal.elems.size() == 1;
                    if (isLeft) {
                        startId = oldVal.elems.get(0)
                            .setScale(0, RoundingMode.HALF_DOWN)
                            .subtract(BigDecimal.ONE);
                    } else {
                        startId = oldVal.elems.get(0)
                            .setScale(0, RoundingMode.HALF_UP)
                            .add(BigDecimal.ONE);
                    }
                    header.len += cnt;
                } else { // The list doesn't exist or expired.
                    if (ifExists) {
                        return null;
                    }
                    startId = BigDecimal.ZERO;
                    // Create new list with cnt elements.
                    header = new ListHeader(cnt);
                }

                CollectionUpdateInfo<ListHeader> opInfo =
                    new CollectionUpdateInfo<>(header);
                opInfo.addPutKeyReq(keyInfo, oldVal);
                
                int end = off + cnt;
                for(int i = off; i < end; i++) {
                    opInfo.addElemReq(makePutElemReq(ki, startId, header.cid,
                        elems[i]));
                    startId = isLeft ? startId.subtract(BigDecimal.ONE) :
                        startId.add(BigDecimal.ONE);
                }

                return opInfo;
            }, (val, res) -> val);
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
        return doMultiUpdate(keyInfo,
            (ki) -> queryListElems(ki, isLeft ? SQL_LPOP : SQL_RPOP,
                new LongValue(cnt)),
            (ki, oldVal) -> makeDeleteListElems(ki, oldVal, true),
            (header, res) -> {
                if (res == null) {
                    return null;
                }

                List<OperationResult> opsRes = res.getResults(); 
                if (opsRes.size() < 2) {
                    throw RedisResponseException.corrupt(
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
                       throw RedisResponseException.corrupt(
                            "Invalid unsuccessful operation result");
                    }
                    MapValue val = opRes.getExistingValue();
                    if (val == null) {
                        throw RedisResponseException.corrupt(
                            "Missing existing value in operation result");
                    }
                    vals.add(new FullBulkStringRedisMessage(
                        getStrVal(rowToElemVal(val))));
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
                    FullBulkStringRedisMessage.NULL_INSTANCE :
                    ArrayRedisMessage.EMPTY_INSTANCE;
            }

            List<RedisMessage> res = doLRPop(keyInfo, cnt, isLeft);
            if (res == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
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
                FullBulkStringRedisMessage.NULL_INSTANCE;
        }
    }

    private RedisMessage handleLRPop(RedisClientContext client,
        RawCommand cmd, boolean isLeft) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        int cnt = cmd.args.length == 2 ?
            (int)Utils.byteBufToLong(cmd.args[1]) : 1;
        if (cnt < 0) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "value out of range, must be positive");
        }

        return handleLRPop(client, cmd.args[0], cnt, isLeft);
    }

    private RedisValueInfoBase<ListHeader> doGetList(RedisKeyInfo keyInfo)
        throws RedisResponseException {
        RedisValueInfo valInfo = doGet(keyInfo);
        return new RedisValueInfoBase<ListHeader>(
            valInfo.isValid() ? new ListHeader(valInfo.val) : null,
            valInfo.ver, valInfo.exp);
    }

    private long doLLen(RedisKeyInfo keyInfo) throws RedisResponseException {
        RedisValueInfoBase<ListHeader> valInfo = doGetList(keyInfo);
        if (!valInfo.isValid()) {
            return 0;
        }
        return valInfo.val.len;
    }

    private List<RedisMessage> doLRange(RedisKeyInfo keyInfo, long off,
        int len, boolean isDesc) throws RedisResponseException {
        return doQuery(keyInfo,
            isDesc ? SQL_LRANGE_DESC : SQL_LRANGE, val ->
                new FullBulkStringRedisMessage(getStrVal(rowToElemVal(val))),
                    new LongValue(len), new LongValue(off));
    }

    private Integer doLRem(RedisKeyInfo keyInfo, ByteBuf val, int cnt,
        boolean isDesc) throws RedisResponseException {
        return doMultiUpdate(keyInfo,
            (ki) -> queryListElems(ki, isDesc ? SQL_LREM_DESC : SQL_LREM,
                new StringValue(makeStrVal(val)), new LongValue(cnt)),
            (ki, oldVal) -> makeDeleteListElems(ki, oldVal, false),
            (header, res) -> {
                if (header == null) {
                    return 0;
                }

                List<OperationResult> opsRes = res.getResults();
                int opCnt = opsRes.size();

                // The first operation result is for list header, the rest is
                // for elements popped.
                if (opCnt < 2) {
                    throw RedisResponseException.corrupt(
                        "Invalid number of delete results: " + opsRes.size());
                }

                assert opsRes.get(0).getSuccess();

                for(int i = 1; i < opCnt; i++) {
                    OperationResult opRes = opsRes.get(i);
                    if (!opRes.getSuccess()) {
                        throw RedisResponseException.corrupt(
                            "Invalid unsuccessful operation result");
                    }
                }

                return Integer.valueOf(opCnt - 1);
            });
    }
    
    // We prefer to trim both ends of the list in one transaction, if
    // possible (number of elements to remove <= MAX_TXN_ELEM_CNT). If not,
    // we do the right side first then left (this choice is arbitrary).
    // To simplify subsequent calls to this function, we convert start and
    // stop to canonical form such that start is non-negative and stop is
    // negative, in this form we know number of elements to query for both
    // left and right queries (since right query is desc). This would only
    // have to be done on the first invocation.
    // Note that start and stop indicate the left and right positions of
    // the elements we keep. The elements we delete (and thus return their ids
    // from this function) are to the left of start and to the right of stop.
    // For the result, we save the earliest version and exp time of the list
    // header we get from get or query requests below (listVal). This way we
    // don't need to worry about list changing concurrently during operations
    // below. If it does change, the update performed by doMultiUpdate() will
    // fail with version mismatch and we retry on the next iteration.
    private CollectionValueInfo<ListTrimInfo, BigDecimal> queryElemsForTrim(
        RedisKeyInfo keyInfo, long start, long stop)
        throws RedisResponseException {
        CollectionValueInfo<ListHeader, BigDecimal> leftRes = null;
        CollectionValueInfo<ListHeader, BigDecimal> rightRes = null;
        long remaining = MAX_TXN_ELEM_CNT;
        RedisValueInfoBase<ListHeader> listVal = null;

        // If start < 0 or stop >= 0, we cannot do either left or right
        // query without knowing the length of the list. If start >= 0 or
        // stop < 0, we can do at least one of left/right queries and which
        // will give us the length of the list to do the other (if start == 0
        // or stop == -1, either left or right trimming is not needed so we
        // don't do corresponding query, but still need to get the list
        // length for the opposite query).
        if (start <= 0 && stop >= -1) {
            // Special case where no trimming on either side is needed.
            if (start == 0 && stop == -1) {
                return CollectionValueInfo.none();
            }
            
            listVal = doGetList(keyInfo);
            if (listVal.val == null) {
                return CollectionValueInfo.none();
            }

            if (start < 0) {
                start = Math.max(start + listVal.val.len, 0);
            }
            if (stop >= 0) {
                stop = Math.min(stop - listVal.val.len, -1);
            }
            // check again after conversion of start and stop
            if (start == 0 && stop == -1) {
                return CollectionValueInfo.none();
            }
        }

        // Do the right side first if possible.
        if (stop < -1) {
            // Last arg is the number of elements to be deleted.
            rightRes = queryListElems(keyInfo, SQL_RPOP,
                new LongValue(Math.min(-stop - 1, remaining)));
            if (rightRes.val == null) {
                return CollectionValueInfo.none();
            }
            if (listVal == null) {
                listVal = rightRes;
            }
            remaining -= rightRes.elems.size();
            // Update value of stop for next invocation.
            stop = Math.max(stop, -rightRes.val.len) +
                rightRes.elems.size();

            // Convert start to canonical form.
            if (start < 0) {
                start = Math.max(start + rightRes.val.len, 0);
            }
        }

        // Do the left side, either because start was already in canonical
        // form or because it was converted to canonical form by getting the
        // list length in the block above.
        if (start > 0 && remaining != 0) {
            leftRes = queryListElems(keyInfo, SQL_LPOP,
                new LongValue(Math.min(start, remaining)));
            if (leftRes.val == null) {
                return CollectionValueInfo.none();
            }
            if (listVal == null) {
                listVal = leftRes;
            }
            remaining -= leftRes.elems.size();
            // Update value of start for next invocation.
            start = Math.min(start, leftRes.val.len - 1) -
                leftRes.elems.size();

            // Convert stop to canonical form.
            // If we could not do the right side before, we can do it now.
            if (stop >= 0) {
                stop = Math.min(stop - leftRes.val.len, -1);
                assert rightRes == null;
                if (stop < -1 && remaining != 0) {
                    rightRes = queryListElems(keyInfo, SQL_RPOP,
                        new LongValue(Math.min(-stop - 1, remaining)));
                    if (rightRes.val == null) {
                        return CollectionValueInfo.none();
                    }
                    // Update value of stop for next invocation.
                    stop = Math.max(stop, -rightRes.val.len) +
                        rightRes.elems.size();
                }
            }
        }

        // Case of start == 0 and stop == -1 is already handled above.
        assert listVal != null;

        CollectionValueInfo<ListTrimInfo, BigDecimal> res =
            new CollectionValueInfo<>(
                new ListTrimInfo(listVal.val.len, start, stop), listVal.ver,
                listVal.exp);
        
        if (leftRes != null) {
            res.elems.addAll(leftRes.elems);
        }
        if (rightRes != null) {
            res.elems.addAll(rightRes.elems);
        }
        
        return res;
    }

    private boolean doTrim(RedisKeyInfo keyInfo, long start, long stop)
        throws RedisResponseException {
        // Use array to avoid issue with using non-effectively-final
        // variables in lambda.
        long [] bounds = { start, stop };

        // Here success means trim was performed (there were list elements to
        // be trimmed).
        boolean success = false;
        do {
            ListTrimInfo trimRes = doMultiUpdate(keyInfo,
                (ki) -> queryElemsForTrim(keyInfo, bounds[0], bounds[1]),
                (ki, oldVal) -> makeDeleteListElems(keyInfo, oldVal, false),
                (trimInfo, res) -> trimInfo);

            if (trimRes == null) {
                return success;
            }

            success = true;
            bounds[0] = trimRes.start;
            bounds[1] = trimRes.stop;
        } while(bounds[0] != 0 || bounds[1] != -1);

        return success;
    }

    private RedisMessage doLPos(RedisKeyInfo keyInfo, ByteBuf val, long rank,
        int count, long maxLen) throws RedisResponseException {
        boolean isDesc = rank < 0;
        if (isDesc) {
            rank = -rank;
        }
        boolean hasCount = count != -1;
        if (!hasCount) {
            count = 1;
        }
        String sql = !isDesc ? (maxLen == 0 ? SQL_LPOS : SQL_LPOS_MAXLEN) :
            (maxLen == 0 ? SQL_LPOS_DESC : SQL_LPOS_DESC_MAXLEN);
        LPosState lpState = new LPosState(val, rank, count, isDesc);
        if (maxLen == 0) {
            processQuery(keyInfo, sql, (row) -> lpState.applyRow(row));
        } else {
            processQuery(keyInfo, sql, (row) -> lpState.applyRow(row),
                new LongValue(maxLen));
        }

        List<Long> matches = lpState.getMatches();
        int numMatches = matches.size();
        if (numMatches == 0) {
            return hasCount ?
                ArrayRedisMessage.EMPTY_INSTANCE :
                FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        if (isDesc) {
            long len = lpState.getLen();
            assert len > 0;
            // reverse the indexes to count from the start of the list
            for(int i = 0; i < numMatches; i++) {
                matches.set(i, len - 1 - matches.get(i));
            }
        }

        if (hasCount) {
            assert numMatches <= count;
            ArrayList<RedisMessage> ls = new ArrayList<>();
            matches.forEach((elem) -> ls.add(new FullBulkStringRedisMessage(
                Utils.longToByteBuf(elem))));
            return new ArrayRedisMessage(ls);
        }

        assert numMatches == 1;
        return new FullBulkStringRedisMessage(
            Utils.longToByteBuf(matches.get(0)));
    }

    private RedisMessage doLInsert(RedisKeyInfo keyInfo, ByteBuf pivot,
        ByteBuf val, boolean isBefore) throws RedisResponseException {
        return doMultiUpdate(keyInfo, (ki) -> {
            CollectionValueInfo<ListHeader, BigDecimal> res =
                queryListElems(ki, SQL_LINSERT_PIVOT, true,
                new StringValue(makeStrVal(pivot)));
            if (res.elems.isEmpty()) {
                // either the list itself or the pivot is not found
                return res;
            }
            List<MapValue> rows = doQuery(ki,
                isBefore ? SQL_LINSERT_BEFORE : SQL_LINSERT_AFTER,
                new NumberValue(res.elems.get(0)));
            if (!rows.isEmpty()) {
                chkSingleResult(rows);
                res.elems.add(rowToElemId(rows.get(0)));
            }
            // res should have at most 2 elements - the pivot and the
            // element before/after if exists.
            return res;
        }, (ki, oldVal) -> {
            ListHeader header = oldVal.val;
            if (header == null) { // list is not found
                return null;
            }
            
            header.len++;

            CollectionUpdateInfo<ListHeader> opInfo =
                new CollectionUpdateInfo<>(header);
            if (oldVal.elems.isEmpty()) {
                return opInfo; // pivot is not found
            }
            
            BigDecimal pivotId = oldVal.elems.get(0);
            BigDecimal newId;

            if (oldVal.elems.size() == 1) {
                // Inserting at either end of the list, same as for
                // LPUSH/RPUSH.
                newId = isBefore ?
                    pivotId.setScale(0, RoundingMode.HALF_DOWN)
                        .subtract(BigDecimal.ONE) :
                    pivotId.setScale(0, RoundingMode.HALF_UP)
                        .add(BigDecimal.ONE);
            } else {
                // Inserting between 2 elements. Try to insert at the
                // mid-point, appropriately rounded.
                BigDecimal otherId = oldVal.elems.get(1);
                newId = pivotId.add(otherId).divide(VALUE_TWO);
                if (newId.scale() > ELEM_ID_PREF_MAX_SCALE) {
                    // Rounding mode shouldn't matter here, we only aim to
                    // get a value distinct from the 2 elements.
                    BigDecimal roundedNewId = newId.setScale(
                        ELEM_ID_PREF_MAX_SCALE, RoundingMode.HALF_EVEN);
                    if (roundedNewId.compareTo(pivotId) == 0 ||
                        roundedNewId.compareTo(otherId) == 0) {
                        // If we could not find distinct point using preferred
                        // max scale, try with absolute max scale, but make
                        // make sure we run do reindexing at this place after
                        // this op.
                        // TODO: trigger reindexing job after this op
                        roundedNewId = newId.setScale(ELEM_ID_MAX_SCALE,
                            RoundingMode.HALF_EVEN);
                        if (roundedNewId.compareTo(pivotId) == 0 ||
                            roundedNewId.compareTo(otherId) == 0) {
                            // TODO: trigger and/or wait for current
                            // reindexing job to complete with timeout before
                            // giving up. If complete, retry this op.
                            throw new RedisResponseException(ErrorPrefix.ERR,
                                "need to reindex list");
                        }
                    }
                    newId = roundedNewId;
                }
            }

            opInfo.addPutKeyReq(ki, oldVal);
            opInfo.addElemReq(makePutElemReq(ki, newId, header.cid, val));
            return opInfo;
        }, (header, res) -> {
            if (res == null) {
                // header == null - list not found
                // header != null - pivot not found
                return header == null ? zeroReply : minusOneReply; 
            }
            assert header != null;
            // On success return list length after insert.
            return new IntegerRedisMessage(header.len);
        });
    }

    protected String getElemsTblName() { return LIST_TABLE_NAME; }
    protected String getSQLSelElems() { return SQL_SEL_ELEMS; }
    protected String getSQLDelElems() { return SQL_DEL_ELEMS; }

    protected CollectionHeader doCopyHeader(RedisValueInfo oldVal, String cid,
        long cnt) throws RedisResponseException {
        return new ListHeader(cid, cnt);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_LPUSH, this::handleLPush);
        cmdMap.put(CMD_RPUSH, this::handleRPush);
        cmdMap.put(CMD_LPUSHX, this::handleLPushX);
        cmdMap.put(CMD_RPUSHX, this::handleRPushX);
        cmdMap.put(CMD_LPOP, this::handleLPop);
        cmdMap.put(CMD_RPOP, this::handleRPop);
        cmdMap.put(CMD_LMPOP, this::handleLMPop);
        cmdMap.put(CMD_LLEN, this::handleLLen);
        cmdMap.put(CMD_LINDEX, this::handleLIndex);
        cmdMap.put(CMD_LRANGE, this::handleLRange);
        cmdMap.put(CMD_LSET, this::handleLSet);
        cmdMap.put(CMD_LREM, this::handleLRem);
        cmdMap.put(CMD_LTRIM, this::handleLTrim);
        cmdMap.put(CMD_LPOS, this::handleLPos);
        cmdMap.put(CMD_LINSERT, this::handleLInsert);
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
        chkMinNumArgs(cmd, 3);
        long numKeys = Utils.byteBufToLong(cmd.args[0]);
        if (numKeys <= 0) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "numkeys should be greater than 0");
        }

        // Total count also includes numkeys and LEFT/RIGHT. Incidentally,
        // this will also check that numKeys <= Integer.MAX_VALUE.
        if (cmd.args.length < numKeys + 2) {
            throw RedisResponseException.syntaxError();
        }

        String leftRight = Utils.byteBufToString(cmd.args[(int)numKeys + 1]);
        boolean isLeft = leftRight.equalsIgnoreCase("LEFT");
        if (!isLeft && !leftRight.equalsIgnoreCase("RIGHT")) {
            throw RedisResponseException.syntaxError();
        }

        long cnt = 1;
        // The only args following LEFT/RIGHT can be: COUNT <count_value>.
        if (cmd.args.length > numKeys + 2) {
            if (cmd.args.length != numKeys + 4 ||
                !Utils.byteBufToString(cmd.args[(int)numKeys + 2])
                    .equalsIgnoreCase("COUNT")) {
                throw RedisResponseException.syntaxError();
            }
            cnt = Utils.byteBufToLong(cmd.args[(int)numKeys + 3]);
            if (cnt <= 0) {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "count should be greater than 0");
            }
        }

        // Note that we cannot have atomicity across different list keys
        // (since they could be on different shards).
        for(int i = 0; i < numKeys; i++) {
            ByteBuf keyBuf = cmd.args[i + 1];
            RedisMessage res = handleLRPop(client, keyBuf, (int)cnt, isLeft);
            if (res != FullBulkStringRedisMessage.NULL_INSTANCE) {
                assert res instanceof ArrayRedisMessage;
                return new ArrayRedisMessage(Arrays.asList(
                    new FullBulkStringRedisMessage(keyBuf), res));
            }
        }

        return FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleLLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return new IntegerRedisMessage(doLLen(makeRedisKeyInfo(cmd.args[0])));
    }

    public RedisMessage handleLIndex(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        long idx = Utils.byteBufToLong(cmd.args[1]);
        boolean isDesc = idx < 0;
        if (isDesc) {
            idx = -idx;
        }

        List<RedisMessage> res = doLRange(makeRedisKeyInfo(cmd.args[0]),
            idx, 1, isDesc);
        return !res.isEmpty() ?
            res.get(0) : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleLRange(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        long start = Utils.byteBufToLong(cmd.args[1]);
        long stop = Utils.byteBufToLong(cmd.args[2]);
        
        // index to start with (from the end for reverse tranversal)
        long startWith;
        boolean isDesc; // whether to do forward or reverse traversal

        // If the signs of start and stop are the same, we can do either
        // forward or reverse traversal and we don't need to query for the
        // length of the list.
        if ((start >= 0 && stop >= 0) || (start < 0 && stop < 0)) {
            if (stop < start) { // holds whether start, stop are + or -
                return ArrayRedisMessage.EMPTY_INSTANCE;
            }
            isDesc = start < 0;
            if (isDesc) {
                startWith = -stop - 1;
            } else {
                startWith = start;
            }
        } else {
            // If signs of start and stop are different, we have to know the
            // length of the list.
            long len = doLLen(keyInfo);

            // For simplicity, first convert to forward indexes.
            if (start < 0) {
                start = Math.max(len + start, 0);
            }
            if (stop < 0) {
                stop = len + stop;
                // If stop is before beginning of list, the result is empty.
                if (stop < 0) {
                    return ArrayRedisMessage.EMPTY_INSTANCE;
                }
            } else {
                stop = Math.min(stop, len - 1);
            }
            if (start >= len || stop < start) {
                return ArrayRedisMessage.EMPTY_INSTANCE;
            }

            // To optimize, we do forward or reverse traversal depending on
            // whether either end of the range is closer to beginning or end
            // of the list.
            isDesc = start > len - stop - 1;
            startWith = isDesc ? len - stop - 1 : start;
        }

        List<RedisMessage> res = doLRange(keyInfo, startWith,
            (int)(stop - start) + 1, isDesc);
        if (isDesc) {
            Collections.reverse(res);
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleLSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        long idx = Utils.byteBufToLong(cmd.args[1]);

        return doMultiUpdate(makeRedisKeyInfo(cmd.args[0]), (ki) -> {
            List<MapValue> rows = doQuery(ki,
                idx >= 0 ? SQL_LSET : SQL_LSET_DESC,
                new LongValue(Math.abs(idx)));

            if (rows.isEmpty()) {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "index out of range");
            }
            chkSingleResult(rows);

            MapValue row0 = rows.get(0);
            CollectionValueInfo<ListHeader, BigDecimal> res =
                new CollectionValueInfo<>(new ListHeader(rowToValue(row0)),
                oracle.nosql.driver.Version.createVersion(rowToVer(row0)),
                getExpTime(rowToKey(row0)));
            BigDecimal elemId = rowToElemId(row0);
            assert elemId != null;
            res.elems.add(elemId);

            return res;
            }, (ki, oldVal) -> {
                ListHeader header = oldVal.val;
                // This may happen if the list has expired.
                if (header == null) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "index out of range");
                }
                
                CollectionUpdateInfo<ListHeader> opInfo =
                    new CollectionUpdateInfo<>(header);
                opInfo.addPutKeyReq(ki, oldVal);
                assert oldVal.elems.size() == 1;
                opInfo.addElemReq(makePutElemReq(ki, oldVal.elems.get(0),
                    header.cid, cmd.args[2]));
                return opInfo;
            }, (header, res) -> okReply);
    }

    public RedisMessage handleLRem(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        
        long cnt = Utils.byteBufToLong(cmd.args[1]);
        boolean isDesc = cnt < 0;
        if (isDesc) {
            cnt = -cnt;
        }
        if (cnt == 0) {
            cnt = Long.MAX_VALUE;
        }

        ByteBuf val = cmd.args[2];
        
        long res;
        
        if (cnt != 0 && cnt < MAX_TXN_ELEM_CNT) {
            res = doLRem(keyInfo, val, (int)cnt, isDesc);
        } else {
            res = 0;
            do {
                long numRem = Math.min(cnt, MAX_TXN_ELEM_CNT);
                long res1 = doLRem(keyInfo, val, (int)numRem, isDesc);
                res += res1;
                // We removed less than requested, which means no more of
                // elements with requested value is left in the list.
                if (res1 < numRem) {
                    break;
                }
                cnt -= res1;
            } while (cnt > 0);
        }

        return new IntegerRedisMessage(res);
    }

    public RedisMessage handleLTrim(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        doTrim(keyInfo, Utils.byteBufToLong(cmd.args[1]),
            Utils.byteBufToLong(cmd.args[2]));
        return okReply;
    }

    public RedisMessage handleLPos(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkNumArgs(cmd, 2, 8);
        // must have even number of args
        if ((cmd.args.length & 1) != 0) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        long rank = 1;
        // -1 to differentiate the case where count was not provided
        long count = -1;
        long maxLen = 0;

        for(int i = 2; i < cmd.args.length; i += 2) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("RANK")) {
                rank = Utils.byteBufToLong(cmd.args[i + 1]);
                if (rank == 0) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "RANK can't be zero: use 1 to start from the first " +
                        "match, 2 from the second, ...");
                }
            } else if (arg.equalsIgnoreCase("COUNT")) {
                count = Utils.byteBufToLong(cmd.args[i + 1]);
                if (count < 0) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "COUNT can't be negative");
                }
                // We can only support 32-bit value of count (Java collection
                // size is an int).
                if (count > Integer.MAX_VALUE) {
                    count = Integer.MAX_VALUE;
                }
            } else if (arg.equalsIgnoreCase("MAXLEN")) {
                maxLen = Utils.byteBufToLong(cmd.args[i + 1]);
                if (maxLen < 0) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        "MAXLEN can't be negative");
                }
            }
        }

        return doLPos(makeRedisKeyInfo(cmd.args[0]), cmd.args[1], rank,
            (int)count, maxLen);
    }

    public RedisMessage handleLInsert(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 4);
        String beforeAfter = Utils.byteBufToString(cmd.args[1]);
        boolean isBefore = beforeAfter.equalsIgnoreCase("BEFORE");
        if (!isBefore && !beforeAfter.equalsIgnoreCase("AFTER")) {
            throw RedisResponseException.syntaxError();
        }

        return doLInsert(makeRedisKeyInfo(cmd.args[0]), cmd.args[2],
            cmd.args[3], isBefore);
    }

}
