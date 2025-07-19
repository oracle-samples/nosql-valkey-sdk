/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.*;

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
import oracle.nosql.driver.values.*;
import oracle.nosql.redis.NoSQLRedisServer;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;
import oracle.nosql.redis.util.Utils.ThrowingFunction;
import oracle.nosql.redis.util.Utils.ThrowingTriFunction;

abstract class CollectionCommandsBase extends CommandsBase {

    private Random rnd = new Random();

    private static final int ABANDONED_CIDS_MAX_ELEMS = 100;

    protected static final String FLD_CID = "cid";
    protected static final String FLD_LEN = "len";

    protected static final String SQL_CID = "$cid";
    protected static final String SQL_SEL_ELEMS_FMT =
        "DECLARE $slot INTEGER; $id STRING; $cid STRING; SELECT * FROM %s " +
        "WHERE slot = $slot AND id = $id AND cid = $cid";
    protected static final String SQL_DEL_ELEMS_FMT =
        "DECLARE $slot INTEGER; $id STRING; $cid STRING; DELETE FROM %s " +
        "WHERE slot = $slot AND id = $id AND cid = $cid";
    protected static final String SQL_SEL_ABANDONED_CIDS_FMT =
        "SELECT $t.cid AS cid FROM %s $t LEFT OUTER JOIN redis $r ON " +
        "$t.slot = $r.slot AND $t.id = $r.id AND $t.cid != $r.value.cid";
    protected static final String SQL_DEL_BY_CIDS_FMT =
        "DELETE FROM %s $t WHERE $t.cid IN $cids[]";

    protected static class CollectionHeader {

        // Collection id (CID) is a UUID column in a collection child table and
        // is also stored as part of collection header as part of "value" field
        // in parent table "redis". It is used to match child element rows to
        // the collection header. We cannot rely only on primary key
        // (slot, keyId) to ensure given child row belongs to the same
        // collection because the collection can be deleted by concurrent
        // transaction and new one created with the same key (slot, keyId) and
        // this cannot always be done atomically (since writeMultiple has a
        // limit on number of rows updated). Since UUID is unique, the same one
        // will never be reused for new collection, so by matching CID of
        // element rows with CID in the collection header we know for sure
        // whether given elements belong to the given collection.
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

    // TODO: planning to remove this class as well as doMultiUpdate, since it
    // doesn't scale well with multiple keys. Switch to using doWithRetries()
    // instead, in this case this class will become unnecessary.
    // Contains information needed to perform an update operation together
    // with the value of the new collection header (dependent on collection
    // type).
    protected static class CollectionUpdateInfo {
        protected final RedisKeyInfo keyInfo;
        protected final RedisValueInfo oldVal;
        protected final long currTime;
        final WriteMultipleRequest wmReq = new WriteMultipleRequest();
        protected PutRequest putKeyReq;
        protected DeleteRequest delKeyReq;

        CollectionUpdateInfo(RedisKeyInfo keyInfo, RedisValueInfo oldVal,
            long currTime) {
            assert keyInfo != null && oldVal != null;
            this.keyInfo = keyInfo;
            this.oldVal = oldVal;
            this.currTime = currTime;
        }

        boolean needUpdate() {
            return wmReq.getNumOperations() != 0;
        }

        // Add put request for the collection header.
        CollectionUpdateInfo addPutKeyReq(MapValue newVal) {
            assert putKeyReq == null && delKeyReq == null;
            putKeyReq = makePutKeyReq(keyInfo, newVal, oldVal, currTime);
            wmReq.add(putKeyReq, true);
            return this;
        }

        CollectionUpdateInfo addPutKeyReq(CollectionHeader header) {
            return addPutKeyReq(header.makeValue());
        }

        // Add delete request for the collection header.
        CollectionUpdateInfo addDeleteKeyReq() {
            assert putKeyReq == null && delKeyReq == null;
            delKeyReq = makeDeleteKeyReq(keyInfo, oldVal);
            wmReq.add(delKeyReq, true);
            return this;
        }

        // Add put or delete request for collection element.
        CollectionUpdateInfo addElemReq(Request elemReq) {
            wmReq.add(elemReq, false);
            return this;
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

    // For most transactions, we use one for the ops for the collection header.
    static final int MAX_TXN_ELEM_CNT = MAX_WM_CNT - 1;

    CollectionCommandsBase(NoSQLHandle nosqlHandle,
        RedisServerConfig config, PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    // Make put request for the collection header.
    static PutRequest makePutKeyReq(RedisKeyInfo keyInfo, MapValue newVal,
        RedisValueInfo oldVal, long currTime) {
        MapValue row = new MapValue().put(FLD_SLOT, keyInfo.slot)
            .put(FLD_ID, keyInfo.id).put(FLD_KEY, makeRedisKey(keyInfo))
            .put(FLD_VALUE, newVal);
        PutRequest putReq = new PutRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setValue(row);

        // For atomicity, our update is dependent on current version or
        // on the fact that the key did not exist before the update.
        if (oldVal.ver != null) {
            putReq.setOption(PutRequest.Option.IfVersion);
            putReq.setMatchVersion(oldVal.ver);
        } else {
            putReq.setOption(PutRequest.Option.IfAbsent);
        }

        // If the old key has expired, we treat this as creating a new
        // key, but since we are overwriting existing row, we have to
        // remove any expiration time that may have been previously set.
        if (oldVal.isExpired(currTime)) {
            putReq.setTTL(TimeToLive.DO_NOT_EXPIRE);
        }

        return putReq;
    }

    static PutRequest makePutKeyRequest(RedisKeyInfo keyInfo,
        CollectionHeader header, RedisValueInfo oldVal, long currTime) {
        return makePutKeyReq(keyInfo, header.makeValue(), oldVal, currTime);
    }

    // Make delete request for the collection header.
    static DeleteRequest makeDeleteKeyReq(RedisKeyInfo keyInfo,
        RedisValueInfo oldVal) {
        DeleteRequest delReq = new DeleteRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setKey(makePrimaryKey(keyInfo));

        // We will remove the key even if it is past expiration.
        delReq.setMatchVersion(oldVal.ver);

        return delReq;
    }

    WriteMultipleResult doWM(WriteMultipleRequest wmReq, boolean toRetry)
        throws RedisResponseException {
        try {
            WriteMultipleResult wmRes = nosqlHandle.writeMultiple(wmReq);
            if (wmRes.getSuccess()) {
                return wmRes;
            }

            if (toRetry) {
                throw new Utils.RedisRetryException();
            }
            throw RedisResponseException.nosql(ERR_WM_FAIL);
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    private void doCleanupElemsTable() throws RedisResponseException {
        PreparedStatement pSelStmt = pstmtCache.getByRef(
            String.format(SQL_SEL_ABANDONED_CIDS_FMT, getElemsTblName()));
        HashSet<String> abandonedCIDs = new HashSet<>();
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pSelStmt);
            boolean isDone = false;
            // The reason for breaking up this query via
            // ABANDONED_CIDS_MAX_ELEMS is in case there are very many
            // abandoned CIDs are retrieved, since I m not sure what the
            // performance of the IN predicate in the delete query will be in
            // this case.
            do {
                do {
                    QueryResult res = nosqlHandle.query(qReq);
                    List<MapValue> rows = res.getResults();
                    for (MapValue row : rows) {
                        abandonedCIDs.add(Utils.getStringField(row, FLD_CID));
                    }
                    if (qReq.isDone()) {
                        isDone = true;
                        break;
                    }

                } while (abandonedCIDs.size() < ABANDONED_CIDS_MAX_ELEMS);

                if (abandonedCIDs.isEmpty()) {
                    continue;
                }

                ArrayValue cids = new ArrayValue().addAll(
                    abandonedCIDs.stream().map(val -> new StringValue(val)));
                PreparedStatement pDelStmt = pstmtCache.getByRef(
                    String.format(SQL_DEL_BY_CIDS_FMT, getElemsTblName()));
                pDelStmt.setVariable("$cids", cids);
                // We can also get number of deleted records for logging
                // purposes.
                processQuery(pDelStmt, row -> {
                });
            } while (!isDone);
        }
    }

    static String makeFromLOJ(String tblName, String varName) {
        return String.format(
            "FROM redis $r LEFT OUTER JOIN %s %s ON $r.slot = %s.slot " +
            "AND $r.id = %s.id AND $r.value.cid = %s.cid ", tblName, varName,
            varName, varName, varName);
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

    boolean isCollectionType() {
        return true;
    }

    <R> List<R> doQuery(RedisKeyInfo keyInfo, String sql,
        ThrowingFunction<MapValue, R, RedisResponseException> getResult,
        FieldValue... vars) throws RedisResponseException {

        ArrayList<R> res = new ArrayList<>();
        processQuery(keyInfo, sql, row -> {
            res.add(getResult.apply(row));
            return false;
        }, vars);

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
        PreparedStatement pStmt = pstmtCache.getByRef(sql);
        
        // The first 2 variables are always the key slot and id.
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
        // Other variables start with $var2, $var3, ...
        for(int i = 0; i < vars.length; i++) {
            pStmt.setVariable("$var" + (i + 2), vars[i]);
        }
        
        return super.processQuery(pStmt, applyRow);
    }

    <V, U, R> R doMultiUpdate(RedisKeyInfo keyInfo,
        ThrowingFunction<RedisKeyInfo, CollectionValueResult<V>,
            RedisResponseException> getOldVal,
        ThrowingTriFunction<RedisKeyInfo, V, CollectionUpdateInfo, U,
            RedisResponseException> makeOp,
        ThrowingTriFunction<V, U, WriteMultipleResult, R,
            RedisResponseException> getResult) throws RedisResponseException {

        long ms = 1;
        for(int i = 0; i < config.maxAtomicRetries; i++) {
            CollectionValueResult<V> vi = getOldVal.apply(keyInfo);

            long currTime = System.currentTimeMillis();
            CollectionUpdateInfo ui = new CollectionUpdateInfo(keyInfo,
                vi.val, currTime);
            U opInfo = makeOp.apply(keyInfo,
                vi.val.isValid(currTime) ? vi.data : null, ui);

            if (!ui.needUpdate()) {
                return getResult.apply(vi.data, opInfo, null);
            }

            try {
                WriteMultipleResult res = nosqlHandle.writeMultiple(ui.wmReq);
                if (res.getSuccess()) {
                    return getResult.apply(vi.data, opInfo, res);
                }
                Thread.sleep(ms, rnd.nextInt(1000000));
                ms *= 2;
            } catch(NoSQLException ex) {
                throw RedisResponseException.nosql(ex);
            } catch(InterruptedException iex) {}
        }

        throw RedisResponseException.nosql(
            "Failed to perform atomic read-update sequence after "
                + config.maxAtomicRetries + " tries");
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

    @Override
    protected void doDelElems(RedisKeyInfo keyInfo, RedisValueInfo valInfo)
        throws RedisResponseException{
        String cid = CollectionHeader.getCid(valInfo.val);
        PreparedStatement pStmt = pstmtCache.getByRef(getSQLDelElems());
        
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
        pStmt.setVariable(SQL_CID, new StringValue(cid));

        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            do {
                QueryResult res = nosqlHandle.query(qReq);
                res.getResults();
            } while(!qReq.isDone());
        };
    }

    @Override
    protected void doSetElemsExp(RedisKeyInfo keyInfo, RedisValueInfo valInfo,
        TimeToLive ttl) throws RedisResponseException {
        String cid = CollectionHeader.getCid(valInfo.val);
        PreparedStatement pStmt = pstmtCache.getByRef(getSQLSelElems());

        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
        pStmt.setVariable(SQL_CID, new StringValue(cid));

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        String tblName = getElemsTblName();

        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt);
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                for(MapValue row : qir) {
                    if (wmReq.getNumOperations() == MAX_WM_CNT) {
                        doWM(wmReq, false);
                        wmReq.clear();
                    }
                    wmReq.add(new PutRequest().setTableName(tblName)
                        .setValue(row).setTTL(ttl), true);
                }
            }
        }

        if (wmReq.getNumOperations() != 0) {
            doWM(wmReq, false);
        }
    }

    protected CopyElemsResult doCopyElems(RedisKeyInfo srcKeyInfo,
        RedisValueInfo srcValInfo, RedisKeyInfo dstKeyInfo)
        throws RedisResponseException {
        String cid = CollectionHeader.getCid(srcValInfo.val);
        PreparedStatement pStmt = pstmtCache.getByRef(getSQLSelElems());

        pStmt.setVariable(SQL_SLOT, new IntegerValue(srcKeyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(srcKeyInfo.id));
        pStmt.setVariable(SQL_CID, new StringValue(cid));

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
                        doWM(wmReq, false);
                        cnt += MAX_TXN_ELEM_CNT;
                        wmReq.clear();
                    }
                    
                    // replace primary key - destination key id and new cid
                    row.put(FLD_SLOT, dstKeyInfo.slot);
                    row.put(FLD_ID, dstKeyInfo.id);
                    row.put(FLD_CID, newCid);

                    wmReq.add(new PutRequest().setTableName(tblName)
                        .setValue(row), true);
                }
            }
        }

        // remaining operations in wmReq
        if (wmReq.getNumOperations() != 0) {
            doWM(wmReq, false);
            cnt += wmReq.getNumOperations();
        }

        return new CopyElemsResult(newCid, cnt);
    }

    @Override
    public void cleanupElemsTable() {
        try {
            doCleanupElemsTable();
        } catch (Exception ex) {
            // Todo: reschedule this task for NoSQL exceptions that can be
            // retried, otherwise just log the exception.
        }
    }
}
