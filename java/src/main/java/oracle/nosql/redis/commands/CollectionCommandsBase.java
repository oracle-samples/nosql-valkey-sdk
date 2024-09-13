/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.NoSQLRedisServer;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;
import oracle.nosql.redis.util.Utils.ThrowingFunction;

abstract class CollectionCommandsBase extends CommandsBase {

    // Same as RedisValueInfo but also contains values for collection
    // elements (mostly element keys), which were typically obtained via
    // join query with the collection's child table.
    // TODO: better unify CollectionValueInfo and RedisValueInfo.
    static class CollectionValueInfo<V, E> extends RedisValueInfoBase<V> {
        final List<E> elems = new ArrayList<>();

        private static final CollectionValueInfo<?,?> NONE =
            new CollectionValueInfo<>(null, null, NO_EXP);

        CollectionValueInfo(V val, oracle.nosql.driver.Version ver,
            long exp) {
            super(val, ver, exp);
        }

        // oldVal is provided to return a value that keeps the version and
        // exp time. This is needed when the key is expired and we still need
        // to make update conditional on expired key version. The resulting
        // value has val = null to simplify the handling code.
        @SuppressWarnings("unchecked")
        static <V, E> CollectionValueInfo<V, E> none(
            CollectionValueInfo<V, E> oldVal) {
            return oldVal == null ?
                (CollectionValueInfo<V, E>) NONE :
                new CollectionValueInfo<V,E>(null, oldVal.ver, oldVal.exp);
        }

        static <V, E> CollectionValueInfo<V, E> none() {
            return none(null);
        }
    }

    // Contains information needed to perform an update operation together
    // with the value of the new collection header (dependent on collection
    // type).
    static class CollectionUpdateInfo<V> {

        final WriteMultipleRequest req = new WriteMultipleRequest();
        V newVal; // newVal = null means the collection key is to be deleted

        void setValue(V val) {
            this.newVal = val;
        }

        boolean needUpdate() {
            return req.getNumOperations() != 0;
        }

        // Add put request for the collection header.
        <E> CollectionUpdateInfo<V> addPutKeyReq(RedisKeyInfo keyInfo,
            CollectionValueInfo<V, E> valInfo, MapValue newVal) {
            
            // put or delete key request should always be set first
            assert req.getNumOperations() == 0;

            MapValue row = new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_KEY, makeRedisKey(keyInfo))
                .put(FLD_VALUE, newVal);
            PutRequest putReq = new PutRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setValue(row);

            // For atomicity, our update is dependent on current version or
            // on the fact that the key did not exist before the update.
            if (valInfo.ver != null) {
                putReq.setOption(PutRequest.Option.IfVersion);
                putReq.setMatchVersion(valInfo.ver);                        
            } else {
                putReq.setOption(PutRequest.Option.IfAbsent);
            }

            // Abort writeMultiple if put of the key fails because of
            // ifAbsent or version mismatch.
            req.add(putReq, true);

            return this;
        }

        // Add delete request for the collection header.
        <E> CollectionUpdateInfo<V> addDeleteKeyReq(RedisKeyInfo keyInfo,
            CollectionValueInfo<V, E> valInfo) {

            // put or delete key request should always be set first
            assert req.getNumOperations() == 0;

            DeleteRequest delReq = new DeleteRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setKey(makePrimaryKey(keyInfo));

            // We will remove the key even if it is past expiration.
            delReq.setMatchVersion(valInfo.ver);

            // Abort writeMultiple if delete of the key fails because of
            // version mismatch.
            req.add(delReq, true);

            return this;
        }

        // Add put or delete request for collection element.
        CollectionUpdateInfo<V> addElemReq(Request elemReq) {
            req.add(elemReq, false);
            return this;
        }

        // Used to remove expiration time from an expired key that we are
        // reusing.
        void unexpireKey() {
            assert req.getNumOperations() != 0;
            Request req0 = req.getOperations().get(0).getRequest();
            if (req0 instanceof PutRequest) {
                ((PutRequest)req0).setTTL(TimeToLive.DO_NOT_EXPIRE);
            }
        }

    }

    static final String FLD_VER = "ver";

    // WriteMultipleRequest can do max of 50 ops and we use one for the
    // collection header.
    static final int MAX_TXN_ELEM_CNT = 49;

    CollectionCommandsBase(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    // Retrieves row version from the query result row, where it is returned
    // as "ver" field.
    static byte[] rowToVer(MapValue row) throws RedisResponseException {
        FieldValue val = row.get(FLD_VER);
        if (val == null || !val.isBinary()) {
            throw RedisResponseException.corrupt("Invalid row version");
        }
        return val.getBinary();
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

    <V, E, R> R doMultiUpdate(RedisKeyInfo keyInfo,
        ThrowingFunction<RedisKeyInfo, CollectionValueInfo<V, E>,
            RedisResponseException> getOldVal,
        ThrowingBiFunction<RedisKeyInfo, CollectionValueInfo<V, E>,
            CollectionUpdateInfo<V>, RedisResponseException> makeOp,
        ThrowingBiFunction<V, WriteMultipleResult, R, RedisResponseException>
            getResult) throws RedisResponseException {

        long ms = 20;
        for(int i = 0; i < ATOMIC_SET_TRIES; i++) {
            CollectionValueInfo<V, E> oldVal = getOldVal.apply(keyInfo);
            
            long currTime = System.currentTimeMillis();

            CollectionUpdateInfo<V> opInfo = makeOp.apply(keyInfo,
                oldVal.isValid(currTime) ? oldVal :
                    CollectionValueInfo.none(oldVal));

            if (opInfo == null || !opInfo.needUpdate()) {
                return getResult.apply(opInfo == null ? null : opInfo.newVal,
                    null);
            }

            // If the old key has expired, we treat this as creating a new
            // key, but since we are overwriting existing row, we have to
            // remove any expiration time that may have been previously set.
            if (!oldVal.isExpired(currTime)) {
                opInfo.unexpireKey();
            }

            try {
                WriteMultipleResult res = nosqlHandle.writeMultiple(
                    opInfo.req);
                if (res.getSuccess()) {
                    return getResult.apply(opInfo.newVal, res);
                }
                Thread.sleep(ms);
                ms *= 2;
            } catch(NoSQLException ex) {
                throw RedisResponseException.nosql(ex);
            } catch(InterruptedException iex) {}
        }

        throw new RedisResponseException(ErrorPrefix.NOSQL,
            "Failed to perform atomic read-update sequence after "
                + ATOMIC_SET_TRIES + " tries");
    }

}
