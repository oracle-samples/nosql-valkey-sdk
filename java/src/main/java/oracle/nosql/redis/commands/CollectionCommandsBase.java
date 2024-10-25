/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.NoSQLRedisServer;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;
import oracle.nosql.redis.util.Utils.ThrowingFunction;
import oracle.nosql.redis.util.Utils.ThrowingTriFunction;

abstract class CollectionCommandsBase extends CommandsBase {

    protected static final String FLD_CID = "cid";
    protected static final String FLD_VER = "ver";
    protected static final String FLD_LEN = "len";

    protected static final String NOT_EXPIRED =
        "AND (NOT EXISTS $r.key.exp OR $r.key.exp > current_time_millis()) ";

    protected static final String SQL_SEL_ELEMS_FMT =
        "DECLARE $var1 STRING; $var2 STRING; SELECT * FROM %s " +
        "WHERE id = $var1 AND cid = $var2";
    protected static final String SQL_DEL_ELEMS_FMT =
        "DECLARE $var1 STRING; $var2 STRING; DELETE FROM %s WHERE " +
        "id = $var1 AND cid = $var2";

    protected static class CollectionHeader {

        String cid;

        CollectionHeader(String cid) {
            this.cid = cid != null ? cid : UUID.randomUUID().toString();
        }

        CollectionHeader() {
            this((String)null);
        }

        CollectionHeader(MapValue val) throws RedisResponseException {
            cid = getCid(val);
        }

        static String getCid(MapValue val) throws RedisResponseException {
            FieldValue fldCid = val.get(FLD_CID);
            if (fldCid == null || !fldCid.isString()) {
                throw RedisResponseException.corrupt(
                    "Missing or invalid value cid field");
            }
    
            String cid = fldCid.getString();
            if (cid == null || cid.isEmpty()) {
                throw RedisResponseException.corrupt("Invalid cid");
            }

            return cid;
        }

        MapValue makeValue() {
            return new MapValue().put(FLD_CID, cid);
        }

    }

    // Combine RedisValueInfo for the collection header with any custom data
    // to be returned in the first callback of doMultiUpdate().
    protected static class CollectionValueResult<V> {
        final RedisValueInfo val;
        final V data;

        CollectionValueResult(RedisValueInfo val, V data) {
            this.val = val;
            this.data = data;
        }

        private static CollectionValueResult<?> NONE =
            new CollectionValueResult<>(RedisValueInfo.NONE, null);

        @SuppressWarnings("unchecked")
        static <T> CollectionValueResult<T> none() {
            return (CollectionValueResult<T>)NONE;
        }
    }

    // Contains information needed to perform an update operation together
    // with the value of the new collection header (dependent on collection
    // type).
    protected static class CollectionUpdateInfo {
        protected final RedisKeyInfo keyInfo;
        protected final RedisValueInfo oldVal;
        final WriteMultipleRequest wmReq = new WriteMultipleRequest();
        protected PutRequest putKeyReq;
        protected DeleteRequest delKeyReq;

        CollectionUpdateInfo(RedisKeyInfo keyInfo, RedisValueInfo oldVal) {
            assert keyInfo != null && oldVal != null;
            this.keyInfo = keyInfo;
            this.oldVal = oldVal;
        }

        boolean needUpdate() {
            return wmReq.getNumOperations() != 0;
        }

        // Add put request for the collection header.
        CollectionUpdateInfo addPutKeyReq(MapValue newVal) {
            assert putKeyReq == null && delKeyReq == null;

            MapValue row = new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_KEY, makeRedisKey(keyInfo)).put(FLD_VALUE, newVal);
            putKeyReq = new PutRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setValue(row);

            // For atomicity, our update is dependent on current version or
            // on the fact that the key did not exist before the update.
            if (oldVal.ver != null) {
                putKeyReq.setOption(PutRequest.Option.IfVersion);
                putKeyReq.setMatchVersion(oldVal.ver);                
            } else {
                putKeyReq.setOption(PutRequest.Option.IfAbsent);
            }

            // Abort writeMultiple if put of the key fails because of
            // ifAbsent or version mismatch.
            wmReq.add(putKeyReq, true);
            return this;
        }

        CollectionUpdateInfo addPutKeyReq(CollectionHeader header) {
            return addPutKeyReq(header.makeValue());
        }

        // Add delete request for the collection header.
        CollectionUpdateInfo addDeleteKeyReq() {
            assert putKeyReq == null && delKeyReq == null;
            
            DeleteRequest delReq = new DeleteRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setKey(makePrimaryKey(keyInfo));

            // We will remove the key even if it is past expiration.
            delReq.setMatchVersion(oldVal.ver);

            // Abort writeMultiple if delete of the key fails because of
            // version mismatch.
            wmReq.add(delReq, true);

            return this;
        }

        // Add put or delete request for collection element.
        CollectionUpdateInfo addElemReq(Request elemReq) {
            wmReq.add(elemReq, false);
            return this;
        }

        // Used to remove expiration time from an expired key that we are
        // reusing.
        void unexpireKey() {
            if (putKeyReq != null) {
                putKeyReq.setTTL(TimeToLive.DO_NOT_EXPIRE);
            }
        }

    }

    protected static class CopyElemsResult {
        final String cid;
        final long cnt;

        CopyElemsResult(String cid, long cnt) {
            this.cid = cid;
            this.cnt = cnt;
        }
    }

    // WriteMultipleRequest can do max of 50 ops.
    static final int MAX_WM_CNT = 50;
    // For most transactions, we use one for the ops for the collection header.
    static final int MAX_TXN_ELEM_CNT = MAX_WM_CNT - 1;

    CollectionCommandsBase(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private void doWM(WriteMultipleRequest wmReq)
        throws RedisResponseException {
        WriteMultipleResult wmRes =
        nosqlHandle.writeMultiple(wmReq);
        if (!wmRes.getSuccess()) {
            // this should not happen but just in case
            throw RedisResponseException.nosql("Unsuccessful writeMultiple");
        }
    }

    // Retrieves row version from the query result row, where it is returned
    // as "ver" field.
    static oracle.nosql.driver.Version rowToVer(MapValue row)
        throws RedisResponseException {
        FieldValue val = row.get(FLD_VER);
        if (val == null || !val.isBinary()) {
            throw RedisResponseException.corrupt("Invalid row version");
        }
        return oracle.nosql.driver.Version.createVersion(val.getBinary());
    }

    static long fvToLen(FieldValue fldLen) throws RedisResponseException {
        if (fldLen == null || !fldLen.isLong()) {
            throw RedisResponseException.corrupt(
                "Missing or invalid len field");
        }

        long len = fldLen.getLong();
        if (len <= 0) {
            throw RedisResponseException.corrupt(
                "Invalid length: " + len);
        }

        return len;
    }

    static void chkSingleResult(List<?> res) throws RedisResponseException {
        if (res.size() != 1) {
            throw RedisResponseException.nosql(
                "Expected single result, got multiple");
        }
    }

    <R> List<R> doQuery(RedisKeyInfo keyInfo, String sql,
        ThrowingFunction<MapValue, R, RedisResponseException> getResult,
        FieldValue... vars) throws RedisResponseException {
        PreparedStatement pStmt = pstmtCache.get(sql);
        
        // The first variable is always the key id.
        pStmt.setVariable("$var1", new StringValue(keyInfo.id));
        for(int i = 0; i < vars.length; i++) {
            pStmt.setVariable("$var" + (i + 2), vars[i]);
        }

        ArrayList<R> res = new ArrayList<>();
        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                for(MapValue row : qir) {
                    res.add(getResult.apply(row));
                }
            }
        }

        return res;
    }

    List<MapValue> doQuery(RedisKeyInfo keyInfo, String sql,
        FieldValue... vals) throws RedisResponseException {
        return doQuery(keyInfo, sql, (val) -> val, vals);
    }

    // Have to change name because of ambiguity with doQuery() that uses
    // ThrowingFunction above.
    // applyRow() will return true if we are done and should not process
    // remaining rows, false otherwise.
    // Return value - true if applyRow() returned true, false otherwise.
    boolean processQuery(RedisKeyInfo keyInfo, String sql,
        ThrowingFunction<MapValue, Boolean, RedisResponseException> applyRow,
        FieldValue... vars) throws RedisResponseException {
        PreparedStatement pStmt = pstmtCache.get(sql);
        
        // The first variable is always the key id.
        pStmt.setVariable("$var1", new StringValue(keyInfo.id));
        for(int i = 0; i < vars.length; i++) {
            pStmt.setVariable("$var" + (i + 2), vars[i]);
        }

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
        }

        return false;
    }

    <V, U, R> R doMultiUpdate(RedisKeyInfo keyInfo,
        ThrowingFunction<RedisKeyInfo, CollectionValueResult<V>,
            RedisResponseException> getOldVal,
        ThrowingTriFunction<RedisKeyInfo, V, CollectionUpdateInfo, U,
            RedisResponseException> makeOp,
        ThrowingTriFunction<V, U, WriteMultipleResult, R,
            RedisResponseException> getResult) throws RedisResponseException {

        long ms = 20;
        for(int i = 0; i < ATOMIC_SET_TRIES; i++) {
            CollectionValueResult<V> vi = getOldVal.apply(keyInfo);

            long currTime = System.currentTimeMillis();
            CollectionUpdateInfo ui = new CollectionUpdateInfo(keyInfo,
                vi.val);
            U opInfo = makeOp.apply(keyInfo,
                vi.val.isValid(currTime) ? vi.data : null, ui);

            if (!ui.needUpdate()) {
                return getResult.apply(vi.data, opInfo, null);
            }

            // If the old key has expired, we treat this as creating a new
            // key, but since we are overwriting existing row, we have to
            // remove any expiration time that may have been previously set.
            if (vi.val.isExpired(currTime)) {
                ui.unexpireKey();
            }

            try {
                WriteMultipleResult res = nosqlHandle.writeMultiple(ui.wmReq);
                if (res.getSuccess()) {
                    return getResult.apply(vi.data, opInfo, res);
                }
                Thread.sleep(ms);
                ms *= 2;
            } catch(NoSQLException ex) {
                throw RedisResponseException.nosql(ex);
            } catch(InterruptedException iex) {}
        }

        throw RedisResponseException.nosql(
            "Failed to perform atomic read-update sequence after "
                + ATOMIC_SET_TRIES + " tries");
    }

    <V, U, R> R doMultiUpdate(RedisKeyInfo keyInfo,
        ThrowingFunction<RedisKeyInfo, CollectionValueResult<V>,
            RedisResponseException> getOldVal,
        ThrowingTriFunction<RedisKeyInfo, V, CollectionUpdateInfo, U,
            RedisResponseException> makeOp,
        ThrowingBiFunction<U, WriteMultipleResult, R,
            RedisResponseException> getResult) throws RedisResponseException {
        return doMultiUpdate(keyInfo, getOldVal, makeOp,
            (data, opInfo, res) -> getResult.apply(opInfo, res));
    }

    // The child table name used by the collection.
    abstract String getElemsTblName();

    // These should be same for every collection, other than the child table
    // name. But we want to use the same reference each time (since
    // PreparedStatementCache looks up by reference), so we instantiate them
    // in subclasses as final.
    abstract String getSQLSelElems();
    abstract String getSQLDelElems();

    protected void doDelElems(RedisKeyInfo keyInfo, RedisValueInfo valInfo)
        throws RedisResponseException{
        String cid = CollectionHeader.getCid(valInfo.val);
        PreparedStatement pStmt = pstmtCache.get(getSQLDelElems());
        
        pStmt.setVariable("$var1", new StringValue(keyInfo.id));
        pStmt.setVariable("$var2", new StringValue(cid));

        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            do {
                @SuppressWarnings("unused")
                QueryResult res = nosqlHandle.query(qReq);
            } while(!qReq.isDone());
        };
    }

    protected void doSetElemsExp(RedisKeyInfo keyInfo, RedisValueInfo valInfo,
        TimeToLive ttl) throws RedisResponseException {
        String cid = CollectionHeader.getCid(valInfo.val);
        PreparedStatement pStmt = pstmtCache.get(getSQLSelElems());

        pStmt.setVariable("$var1", new StringValue(keyInfo.id));
        pStmt.setVariable("$var2", new StringValue(cid));

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        String tblName = getElemsTblName();

        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                for(MapValue row : qir) {
                    if (wmReq.getNumOperations() == MAX_WM_CNT) {
                        doWM(wmReq);
                        wmReq.clear();
                    }
                    wmReq.add(new PutRequest().setTableName(tblName)
                        .setValue(row).setTTL(ttl), true);
                }
            }
        }

        if (wmReq.getNumOperations() != 0) {
            doWM(wmReq);
        }
    }

    protected CopyElemsResult doCopyElems(RedisKeyInfo srcKeyInfo,
        RedisValueInfo srcValInfo, RedisKeyInfo dstKeyInfo)
        throws RedisResponseException {
        String cid = CollectionHeader.getCid(srcValInfo.val);
        PreparedStatement pStmt = pstmtCache.get(getSQLSelElems());

        pStmt.setVariable("$var1", new StringValue(srcKeyInfo.id));
        pStmt.setVariable("$var2", new StringValue(cid));

        String newCid = UUID.randomUUID().toString();

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        String tblName = getElemsTblName();
        
        long cnt = 0;

        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                for(MapValue row : qir) {
                    if (wmReq.getNumOperations() == MAX_WM_CNT) {
                        doWM(wmReq);
                        cnt += MAX_TXN_ELEM_CNT;
                        wmReq.clear();
                    }
                    
                    // replace primary key - destination key id and new cid
                    row.put(FLD_ID, dstKeyInfo.id);
                    row.put(FLD_CID, newCid);

                    wmReq.add(new PutRequest().setTableName(tblName)
                        .setValue(row), true);
                }
            }
        }

        // remaining operations in wmReq
        if (wmReq.getNumOperations() != 0) {
            doWM(wmReq);
            cnt += wmReq.getNumOperations();
        }

        return new CopyElemsResult(newCid, cnt);
    }

}
