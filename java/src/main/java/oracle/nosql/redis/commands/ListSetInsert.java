/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.WriteMultipleRequest;
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
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class ListSetInsert extends ListCommandsBase {

    // Using writeMultiple, can perform max of MAX_TXN_ELEM_CNT puts/deletes.
    // One put is reserved for the list header.  Shifting each elemId requres
    // one delete and one put thus max number of elemIds shifted is half of
    // that.
    private static final int REINDEX_STEP_CNT = (MAX_TXN_ELEM_CNT - 1) / 2;

    private static final int MAX_REINDEX_ATTEMPTS = 3;
    private static final int MAX_LINSERT_ATTEMPS = 3;

    private static final String VAR2_NUM_VAR3_NUM =
        "$var2 NUMBER; $var3 NUMBER; ";
    private static final String REINDEX_LIMIT = " LIMIT " + REINDEX_STEP_CNT;

    private static final String SQL_LSET = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", PK_COLS, LIMIT_1, OFFSET_VAR2);
    private static final String SQL_LSET_DESC = String.format(SQL_ELEMS_FMT,
        VAR2_LONG, "", "", PK_COLS_DESC, LIMIT_1, OFFSET_VAR2);

    // The query to find pivot elemId for LINSERT command.
    // Note that per return value spec, we are required to differentiate
    // between the case when list key does not exist and the case when list
    // exists but the pivot is not found.
    private static final String SQL_LINSERT_PIVOT = DECL_KEY_ID +
        "$var2 STRING; SELECT row_version($r) AS ver, $r.key, $r.value, " +
        "$l.elemId " + FROM_JOIN_WHERE_L_PK + ELEM_VAL_VAR2 + "ORDER BY " +
        PK_COLS + LIMIT_1;

    private static final String SQL_ELEM_IDS_RIGHT = String.format(
        SQL_ELEM_ID_FMT, ">=", PK_COLS, "");
    private static final String SQL_ELEM_IDS_LEFT = String.format(
        SQL_ELEM_ID_FMT, "<=", PK_COLS_DESC, "");

    private static final String SQL_ELEMS_AFTER = String.format(SQL_ELEMS_FMT,
        VAR2_NUM_VAR3_NUM, ELEM_VAL,
        " AND $l.elemId > $var2 AND $l.elemId < $var3 ", PK_COLS,
        REINDEX_LIMIT, "");
    private static final String SQL_ELEMS_BEFORE = String.format(SQL_ELEMS_FMT,
        VAR2_NUM_VAR3_NUM, ELEM_VAL,
        " AND $l.elemId < $var2 AND $l.elemId > $var3 ", PK_COLS_DESC,
        REINDEX_LIMIT, "");

    // Selects 1 element either before or after the pivot (given by its
    // elemId). Unfortunately, since subquery is not supported, we cannot find
    // pivot and the id before/after in a single query.
    private static final String SQL_LINSERT_AFTER = String.format(
        SQL_ELEM_ID_FMT, ">", PK_COLS, LIMIT_1);
    private static final String SQL_LINSERT_BEFORE = String.format(
        SQL_ELEM_ID_FMT, "<", PK_COLS_DESC, LIMIT_1);

    // To avoid unlimited growth of elemId size when there are repeated
    // inserts near the same location, we cap the elemId scale at certain max
    // value when calculating new elemId to insert. This means that in certain
    // rare cases insert will fail as it will not be able to find distinct
    // elemId (see doLInsert()). In this case, we have to reindex to spread
    // element ids. Reindexing will move adjacent ids to a preferred minimum
    // distance from each other (see ELEM_ID_PREF_MIN_DIST) allowing for more
    // inserts. It will also cap their scale at preferred maximum scale (see
    // ELEM_ID_PREF_MAX_SCALE). To avoid frequent reindexing, we use much
    // smaller max scale (and bigger min distance) when reindexing than when
    // doing inserts, hoping that many inserts can take place in the same
    // vicinity before we need to reindex again.
    // To avoid reindexing the whole list, we try to only reindex by spreading
    // the ids near the problematic area. In particular, we need to spread the
    // interval between the two ids that bounded the problematic id (that
    // failed to insert) and then all affected neighboring ids.
    // In simple case, without considering concurrency or durability, we could
    // just start shifting affected ids going in one direction achieving
    // preferred minimum distance between any adjacent ids, until the next id
    // we encounter is already at preferred minimum distance (or above), at
    // which point we can stop. In the worst case, this process would proceed
    // until one of the ends of the list.
    // However, because reindexing would have to be done over multiple
    // requests, the sorted order of the list may be violated between these
    // requests. To preserve the order of the list while shifting the ids, we
    // have to move along the list in the opposite directions of shifting the
    // ids. E.g. we can move from right to left while shifting each id from
    // left to right. For this we use 2 passes. The 1st pass will find the
    // minimum interval to reindex. The 2nd pass will move in the opposite
    // direction of the 1st and shift the ids to the preferred min distance
    // from each other.

    private static final int ELEM_ID_MAX_SCALE = 10;
    // private static final int ELEM_ID_MAX_SCALE = 4;
    private static final int ELEM_ID_PREF_MAX_SCALE = 5;
    // private static final int ELEM_ID_PREF_MAX_SCALE = 2;
    private static final BigDecimal ELEM_ID_PREF_MIN_DIST =
        BigDecimal.ONE.movePointLeft(ELEM_ID_PREF_MAX_SCALE);
    private static final BigDecimal HALF = new BigDecimal(0.5);

    // We use this class to distinguish between 3 cases:
    // 1) Reindex finished, we return null.
    // 2) To continue reindexing we return new ReindexBatchResult(nextFromId).
    // 3) If we have to abort reindexing due to concurrent insert of new
    // elemId in the reindex interval, we return ReindexBatchResult.ABORT.
    private static class ReindexBatchResult {
        final BigDecimal nextFromId;

        static final ReindexBatchResult ABORT = new ReindexBatchResult(null);

        ReindexBatchResult(BigDecimal nextFromId) {
            this.nextFromId = nextFromId;
        }
    }

    private static class InsertResult {
        final RedisMessage res;
        final BigDecimal reindexStartId;

        static final InsertResult LIST_NOT_FOUND =
            new InsertResult(zeroReply, null);
        static final InsertResult PIVOT_NOT_FOUND =
            new InsertResult(minusOneReply, null);

        private InsertResult(RedisMessage res, BigDecimal reindexRestartId) {
                this.res = res;
                this.reindexStartId = reindexRestartId;
            }

        static InsertResult success(ListHeader header) {
            return new InsertResult(new IntegerRedisMessage(header.len),
                null);
        }

        static InsertResult needReindex(BigDecimal reindexStartId) {
            return new InsertResult(null, reindexStartId);
        }
    }

    public ListSetInsert(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    // Shift left but make sure we cap at the max preferable scale by rounding
    // to the left.
    private static BigDecimal reindexShiftLeft(BigDecimal id) {
        id = id.subtract(ELEM_ID_PREF_MIN_DIST);
        return id.scale() > ELEM_ID_PREF_MAX_SCALE ?
            id.setScale(ELEM_ID_PREF_MAX_SCALE, RoundingMode.FLOOR) : id;
    }

    // Same as above, but shift right.
    private static BigDecimal reindexShiftRight(BigDecimal id) {
        id = id.add(ELEM_ID_PREF_MIN_DIST);
        return id.scale() > ELEM_ID_PREF_MAX_SCALE ?
            id.setScale(ELEM_ID_PREF_MAX_SCALE, RoundingMode.CEILING) : id;
    }

    // Reindexing 1st pass.
    // This returns both updated lower and upper bounds (or vice versa if
    // going to the left). The updated lower bound may shorten reindexing
    // interval in the beginning to avoid reindexing where it is not necessary
    // (because the ids are already sufficiently spread apart).
    private BigDecimal[] findReindexInterval(RedisKeyInfo keyInfo,
        BigDecimal fromId, BigDecimal toId, boolean isLeft)
        throws RedisResponseException {
        assert (isLeft && toId.compareTo(fromId) < 0) ||
            (!isLeft && toId.compareTo(fromId) > 0);

        // reindex interval
        BigDecimal [] ri = { fromId, fromId };
        // state[0] - 2nd iteration and after. We choose the first id iterated
        // as the starting point and only shift after it.
        // state[1] - whether we already started to shift - after this moving
        // the lower bound is not possible.
        boolean[] state = { false, false };

        boolean res = processQuery(keyInfo,
            isLeft ? SQL_ELEM_IDS_LEFT : SQL_ELEM_IDS_RIGHT, row -> {
                BigDecimal elemId = rowToElemId(row);
                if (state[0]) {
                    if (isLeft && (ri[1].subtract(elemId)
                        .compareTo(ELEM_ID_PREF_MIN_DIST) < 0)) {
                        ri[1] = reindexShiftLeft(ri[1]);
                        state[1] = true;
                        return false;
                    }
                    if (!isLeft && (elemId.subtract(ri[1])
                        .compareTo(ELEM_ID_PREF_MIN_DIST) < 0)) {
                        ri[1] = reindexShiftRight(ri[1]);
                        state[1] = true;
                        return false;
                    }
                }

                state[0] = true;
                if (!state[1]) {
                    ri[0] = elemId;
                }
                ri[1] = elemId;
                
                // Even if no shift was required, if we haven't passed toId,
                // we still keep going.
                return isLeft ? ri[1].compareTo(toId) <= 0 :
                    ri[1].compareTo(toId) >= 0;
            }, new NumberValue(fromId));

        // If res = false, we are at one of the ends of the list without
        // finding an id at sufficient distance. Since the end boundary is
        // exclusive, we add another preferred min distance to it. We could
        // expand it further.
        if (!res) {
            ri[1] = isLeft ? reindexShiftLeft(ri[1]) :
                reindexShiftRight(ri[1]);
        }

        return ri;
    }

    // Note that the 2nd pass is going in the opposite direction to the 1st
    // pass. isLeft, fromId and toId are relative to the 2nd pass,
    // isLeft = true would mean our 1st pass was from left to right and now
    // we are going back. fromId is the id after which we start each batch of
    // the 2nd pass and is equal to the last processed id of the previous
    // batch. toId is the id at which we stop, which is the same id we have
    // started with at the 1st pass (note that both fromId and toId are
    // exclusive).
    // Return value:
    // rbRes.isDone is true if we are done (there are no more values to
    // reindex) or the list no longer exists. Otherwise, rbRes.nextFromId used
    // to start the next batch. If rbRes.nextFromId is null, the batch was
    // aborted because a new element was inserted concurrently so that the
    // interval computed in pass 1 is no longer valid (see below).
    private ReindexBatchResult doReindexBatch(RedisKeyInfo keyInfo,
        BigDecimal fromId, BigDecimal toId, boolean isLeft)
        throws RedisResponseException {
        return doWithRetries(() -> {
            List<MapValue> rows = doQuery(keyInfo,
                isLeft ? SQL_ELEMS_BEFORE : SQL_ELEMS_AFTER,
                new NumberValue(fromId), new NumberValue(toId));
            if (rows.isEmpty()) {
                // We are done with reindexing or the list no longer exists.
                return null;
            }

            MapValue row0 = rows.get(0);
            RedisValueInfo val = RedisValueInfo.create(rowToValue(row0),
                rowToVer(row0), getExpTime(rowToKey(row0)));
            if (!val.isValid()) {
                return null;
            }

            ListValueInfo lvi = new ListValueInfo(val, rows, true);
            ListHeader header = lvi.header;
            assert !lvi.elemIds.isEmpty();
            assert lvi.elemVals != null;
            assert lvi.elemIds.size() == lvi.elemVals.size();

            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            wmReq.add(makePutKeyReq(keyInfo, header, val), true);

            int cnt = lvi.elemIds.size();
            BigDecimal currId = fromId;

            // We need additional iteration to determine where to add delete
            // requests for the old element ids. This is because new element
            // ids could coinside with old element ids and we cannot add both
            // put and delete request for the same key.
            int delIdx = 0;

            for(int putIdx = 0; putIdx < cnt; putIdx++) {
                BigDecimal elemId = lvi.elemIds.get(putIdx);
                FieldValue elemVal = new StringValue(lvi.elemVals.get(putIdx));

                currId = isLeft ?
                    reindexShiftLeft(currId) : reindexShiftRight(currId);

                // Note that we must always shift forward if going back
                // (isBefore = true) or backward if going forward
                // (isBefore = false).
                if ((isLeft && (currId.compareTo(elemId) < 0)) ||
                    (!isLeft && (currId.compareTo(elemId) > 0))) {
                    // This can only happen if some other client has inserted
                    // new element with elemId after we have done the 1st
                    // pass. In this case, our interval is no longer valid
                    // and we have to abort.

                    // Since we still do partial reindex in this case, make
                    // sure we add delete requests for elements already
                    // processed.
                    for(; delIdx < putIdx; delIdx++) {
                        wmReq.add(makeDeleteElemReq(keyInfo,
                            lvi.elemIds.get(delIdx), false), false);
                    }

                    // check if we should do partial reindex
                    if (wmReq.getNumOperations() > 1) {
                        doWM(wmReq, true);
                    }

                    return ReindexBatchResult.ABORT;
                }

                wmReq.add(makePutElemReq(keyInfo, currId, header.cid,
                    elemVal), false);

                for(; delIdx < cnt; delIdx++) {
                    BigDecimal elemId2 = lvi.elemIds.get(delIdx);
                    int cmpRes = elemId2.compareTo(currId);

                    // We can increment delIdx and avoid next iteration if
                    // ids are equal.
                    if ((cmpRes == 0 && delIdx++ >= 0) ||
                        (isLeft && cmpRes < 0) || (!isLeft && cmpRes > 0)) {
                        break;
                    }

                    // We know ids are not equal here.
                    wmReq.add(makeDeleteElemReq(keyInfo, elemId2, false),
                        false);
                }
            }

            // Complete adding delete requests (we are now past last put
            // element id, so id clash is no longer possible).
            for(; delIdx < cnt; delIdx++) {
                wmReq.add(makeDeleteElemReq(keyInfo,
                    lvi.elemIds.get(delIdx), false), false);
            }

            doWM(wmReq, true);
            return new ReindexBatchResult(currId);
        });
    }

    private void doReindex(RedisKeyInfo keyInfo, BigDecimal startId)
        throws RedisResponseException {
        // It is likely there are a lot of elements with ids near the
        // problematic id within the preferred minumum distance (at least in
        // one direction from problematic id, that's why we have to reindex in
        // the first place). It may be best to reindex within the whole radius
        // of preferred minumum distance in order to avoid repeated reindexing
        // if more inserts fall into the same area.
        BigDecimal leftId = reindexShiftLeft(startId);
        BigDecimal rightId = reindexShiftRight(startId);

        // As a heuristic, we reindex in the direction of more distant whole
        // number, hoping that there is more space to spread reindexed ids in
        // that interval. This may need to be reconsidered. We start with
        // leftId to reindex to the right and vice versa, in order to make more
        // room for the problematic id.
        boolean isLeft = startId.subtract(
            startId.setScale(0, RoundingMode.FLOOR)).compareTo(HALF) > 0;
        
        for(int i = 0; i < MAX_REINDEX_ATTEMPTS; i++) {
            // 1st pass
            BigDecimal fromId1 = isLeft ? rightId : leftId;
            BigDecimal toId1 = isLeft ? leftId : rightId;

            // 2nd pass is in the opposite direction of 1st pass
            BigDecimal [] pass1Res = findReindexInterval(keyInfo, fromId1,
                toId1, isLeft);

            BigDecimal fromId2 = pass1Res[1];
            BigDecimal toId2 = pass1Res[0];
            ReindexBatchResult res;

            for(;;) {
                res = doReindexBatch(keyInfo, fromId2, toId2, !isLeft);
                if (res == null) {
                    // No more ids to process or the list no longer exists, so
                    // we are done.
                    return;
                }
                fromId2 = res.nextFromId;
                if (fromId2 == null) {
                    // The batch was aborted because another client inserted
                    // a new element which invalidated the interval computed
                    // in step 1. In this case, we recompute the interval
                    // and retry, up to MAX_REINDEX_ATTEMPTS.
                    break;
                }
            }
        }
    }

    private InsertResult doLInsert(RedisKeyInfo keyInfo, ByteBuf pivot,
        ByteBuf val, boolean isBefore) throws RedisResponseException {
        return doWithRetries(() -> {
            CollectionValueResult<ListValueInfo> cvr =
                queryListElems(keyInfo, SQL_LINSERT_PIVOT, true,
                    new StringValue(makeStrVal(pivot)));

            // See comments in queryListElems().
            if (!cvr.isValid()) {
                return InsertResult.LIST_NOT_FOUND;
            }

            if (cvr.data.elemIds.isEmpty()) {
                return InsertResult.PIVOT_NOT_FOUND;
            }

            // Note that it is possible that the list would be changed between
            // these two queries, or even deleted (and something else created
            // under the same key). However, we condition any updates on the
            // list row version returned by the first query, so if that
            // happens, no updates will be performed.
            List<MapValue> rows = doQuery(keyInfo,
                isBefore ? SQL_LINSERT_BEFORE : SQL_LINSERT_AFTER,
                new NumberValue(cvr.data.elemIds.get(0)));
            if (!rows.isEmpty()) {
                chkSingleResult(rows);
                cvr.data.elemIds.add(rowToElemId(rows.get(0)));
            }

            ListValueInfo lvi = cvr.data;
            ListHeader header = lvi.header;

            header.len++;

            BigDecimal pivotId = lvi.elemIds.get(0);
            BigDecimal newId;

            // There should be at most 2 elements - the pivot and the
            // element before/after if exists.
            if (lvi.elemIds.size() == 1) {
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
                BigDecimal otherId = lvi.elemIds.get(1);
                newId = pivotId.add(otherId).divide(VALUE_TWO);
                if (newId.scale() > ELEM_ID_MAX_SCALE) {
                    // Rounding mode shouldn't matter here, we only aim to
                    // get a value distinct from the 2 elements.
                    BigDecimal roundedNewId = newId.setScale(
                        ELEM_ID_MAX_SCALE, RoundingMode.HALF_EVEN);
                    if (roundedNewId.compareTo(pivotId) == 0 ||
                        roundedNewId.compareTo(otherId) == 0) {
                        // We cannot fit the id between these two, so we
                        // reindex and retry.
                        return InsertResult.needReindex(newId);
                    }
                    newId = roundedNewId;
                }
            }

            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            wmReq.add(makePutKeyReq(keyInfo, header, cvr.val), true);
            wmReq.add(makePutElemReq(keyInfo, newId, header.cid, val), false);
            doWM(wmReq, true);
            return InsertResult.success(header);
        });
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_LSET, this::handleLSet);
        cmdMap.put(CMD_LINSERT, this::handleLInsert);
    }

    public RedisMessage handleLSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        long idxArg = Utils.byteBufToLong(cmd.args[1]);
        final boolean isAsc = idxArg >= 0;
        // need another variable idx to make it effectively final
        final long idx = isAsc ? idxArg : -idxArg - 1;
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        return doWithRetries(() -> {
            CollectionValueResult<ListValueInfo> cvr =
                queryListElems(keyInfo, isAsc ? SQL_LSET : SQL_LSET_DESC, true,
                    new LongValue(idx));

            // See comments in queryListElems().
            if (!cvr.isValid()) {
                throw RedisResponseException.noSuchKey();
            }

            if (cvr.data.elemIds.isEmpty()) {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "index out of range");
            }

            ListValueInfo lvi = cvr.data;
            ListHeader header = lvi.header;
            assert(header != null);

            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            wmReq.add(makePutKeyReq(keyInfo, header, cvr.val), true);
            assert lvi.elemIds.size() == 1;
            wmReq.add(makePutElemReq(keyInfo, lvi.elemIds.get(0),
                header.cid, cmd.args[2]), false);

            doWM(wmReq, true);
            return okReply;
        });
    }

    public RedisMessage handleLInsert(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 4);
        String beforeAfter = Utils.byteBufToString(cmd.args[1]);
        boolean isBefore = beforeAfter.equalsIgnoreCase("BEFORE");
        if (!isBefore && !beforeAfter.equalsIgnoreCase("AFTER")) {
            throw RedisResponseException.syntaxError();
        }

        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        ByteBuf pivot = cmd.args[2];
        ByteBuf val = cmd.args[3];

        for(int i = 0; i < MAX_LINSERT_ATTEMPS; i++) {
            InsertResult res = doLInsert(keyInfo, pivot, val, isBefore);
            if (res.res != null) {
                return res.res;
            }
            // If we fail to insert because elemId could not fit, we try to
            // reindex at that place and retry the insert. Note that it is
            // possible for retry to also fail if there are concurrent
            // clients inserting elemIds near that location. In this case,
            // we retry the same couple of times and throw if not successful.
            // This should be very rare.
            assert res.reindexStartId != null;
            doReindex(keyInfo, res.reindexStartId);
        }

        throw RedisResponseException.nosql(
            "Failed to reindex list after multiple attempts");
    }

}
