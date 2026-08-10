/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.*;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.*;
import oracle.nosql.valkey.NoSQLRedisServer;
import oracle.nosql.valkey.RedisResponseException;
import oracle.nosql.valkey.RedisServerConfig;
import oracle.nosql.valkey.util.PreparedStatementCache;
import oracle.nosql.valkey.util.Utils;
import oracle.nosql.valkey.util.Utils.ThrowingFunction;

abstract class CollectionCommandsBase extends CommandsBase {

    private Random rnd = new Random();

    private static final int ABANDONED_CIDS_MAX_ELEMS = 100;

    protected static final String FLD_CID = "cid";
    protected static final String FLD_LEN = "len";

    private static final Logger logger =
        Logger.getLogger(CollectionCommandsBase.class.getName());

    protected static final String SQL_CID = "$cid";
    protected static final String SQL_SEL_ELEMS_FMT =
        "DECLARE $slot INTEGER; $id STRING; $cid STRING; SELECT * FROM %s " +
        "WHERE slot = $slot AND id = $id AND cid = $cid";
    protected static final String SQL_DEL_ELEMS_FMT =
        "DECLARE $slot INTEGER; $id STRING; $cid STRING; DELETE FROM %s " +
        "WHERE slot = $slot AND id = $id AND cid = $cid";
    protected static final String SQL_SEL_ABANDONED_CIDS_FMT =
        "SELECT DISTINCT $t.cid AS cid FROM %s $t LEFT OUTER JOIN valkey $r " +
        "ON $t.slot = $r.slot AND $t.id = $r.id WHERE " +
        "$r IS NULL OR $t.cid != $r.value.cid";
    protected static final String SQL_DEL_BY_CIDS_FMT =
        "DECLARE $cids ARRAY(STRING); " +
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
                throw isCollectionType(getValueType(val)) ?
                    RedisResponseException.corrupt(
                        "Missing or invalid value cid field") :
                    RedisResponseException.wrongType();
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
    // to be returned in order to do update.
    protected static class CollectionValueResult<V> {
        final RedisValueInfo val;
        final V data;

        CollectionValueResult(RedisValueInfo val, V data) {
            assert val != null;
            assert data != null || !val.isValid();
            this.val = val;
            this.data = data;
        }

        private static CollectionValueResult<?> NONE =
            new CollectionValueResult<>(RedisValueInfo.NONE, null);

        @SuppressWarnings("unchecked")
        static <T> CollectionValueResult<T> none() {
            return (CollectionValueResult<T>)NONE;
        }

        boolean isValid() {
            return val.isValid();
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

    // So far, we only use this for error handling.
    static boolean isCollectionType(String type) {
        return type.equals(TYPE_LIST) || type.equals(TYPE_HASH) ||
            type.equals(TYPE_SET) || type.equals(TYPE_ZSET) ||
            type.equals(TYPE_STREAM);
    }

    // Make put request for the collection header.
    static PutRequest makePutKeyReq(RedisKeyInfo keyInfo, MapValue newVal,
        RedisValueInfo oldVal) {
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
        if (oldVal.isExpired()) {
            putReq.setTTL(TimeToLive.DO_NOT_EXPIRE);
        }

        return putReq;
    }

    static PutRequest makePutKeyReq(RedisKeyInfo keyInfo,
        CollectionHeader header, RedisValueInfo oldVal) {
        return makePutKeyReq(keyInfo, header.makeValue(), oldVal);
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
        ArrayValue cids = new ArrayValue();
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
                        FieldValue cid = row.get(FLD_CID);
                        if (cid == null || !cid.isString()) {
                            throw RedisResponseException.corrupt(
                                "Missing or invalid cid");
                        }
                        cids.add(cid);
                    }
                    if (qReq.isDone()) {
                        isDone = true;
                        break;
                    }

                } while (cids.size() < ABANDONED_CIDS_MAX_ELEMS);

                if (cids.size() == 0) {
                    continue;
                }

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

    // The child table name used by the collection.
    abstract String getElemsTblName();

    // These should be same for every collection, other than the child table
    // name. But we want to use the same reference each time (since
    // PreparedStatementCache looks up by reference), so we instantiate them
    // in subclasses as final.
    abstract String getSQLSelElems();
    abstract String getSQLDelElems();

    private PreparedStatement getBoundStmt(String sql, RedisKeyInfo keyInfo,
       RedisValueInfo valInfo) throws RedisResponseException {
        String cid = CollectionHeader.getCid(valInfo.val);
        PreparedStatement pStmt = pstmtCache.getByRef(sql);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
        pStmt.setVariable(SQL_CID, new StringValue(cid));
        return pStmt;
    }

    @Override
    protected void doDelElems(RedisKeyInfo keyInfo, RedisValueInfo valInfo)
        throws RedisResponseException{
        processQuery(getBoundStmt(getSQLDelElems(), keyInfo, valInfo));
    }

    @Override
    protected void doSetElemsExp(RedisKeyInfo keyInfo, RedisValueInfo valInfo,
        TimeToLive ttl) throws RedisResponseException {
        PreparedStatement pStmt = getBoundStmt(getSQLSelElems(), keyInfo,
            valInfo);
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
        PreparedStatement pStmt = getBoundStmt(getSQLSelElems(), srcKeyInfo,
            srcValInfo);
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
            logger.warning(String.format(
                "Error cleaning up %s: %s", getElemsTblName(),
                ex.getMessage()));
            // Todo: reschedule this task for NoSQL exceptions that can be
            // retried.
        }
    }
}
