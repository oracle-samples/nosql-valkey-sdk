package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingFunction;

import static oracle.nosql.redis.util.Utils.getStringField;

public class HashUpdate extends HashCommandsBase {
    
    private static class HSetResult {
        final int addedCnt;
        final int processedCnt;

        HSetResult(int addedCnt, int processedCnt) {
            this.addedCnt = addedCnt;
            this.processedCnt = processedCnt;
        }
    }

    private static void chkEntrySize(ByteBuf hKeyBuf, ByteBuf hValBuf)
        throws RedisResponseException {
        if (hKeyBuf.readableBytes() + hValBuf.readableBytes() >
            MAX_ENTRY_LEN) {
            throw new RedisResponseException(
                RedisResponseException.ErrorPrefix.ERR,
                "hash entry exceeds maximum allowed size");
        }
    }

    private static void chkHeaderLength(HashHeader header)
        throws RedisResponseException{
        if (header.len <= 0) {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }
    }

    // We use this function instead of CollectionCommandsBase.doWM() to guard
    // against the case when existing key is of wrong type (not hash). In this
    // case our SQL JOIN (see getExistingVal, getExistingFldVal) will return
    // no records. Instead of always calling chkHashKey() when the query
    // returns no records, we optimize it somewhat by assuming first that the
    // key does not exist (and thus we will use ifAbsent in WM), performing WM
    // and only calling chkHashKey() if WM fails (after which we will perform
    // retries). This way we avoid extra request done by chkHashKey() in
    // then normal case when the key doesn't exist. Unfortunately there is no
    // way to avoid this for read-only commands in order to return correct
    // return value or an error.
    private WriteMultipleResult doChkWM(WriteMultipleRequest wmReq,
        RedisKeyInfo keyInfo, RedisValueInfo oldVal)
        throws RedisResponseException {
        try {
            WriteMultipleResult wmRes = nosqlHandle.writeMultiple(wmReq);
            if (wmRes.getSuccess()) {
                return wmRes;
            }
            if (!oldVal.exists()) {
                chkHashKey(keyInfo);
            }
            throw new Utils.RedisRetryException();
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    private void convertToMultiRow(RedisKeyInfo keyInfo,
        HashHeader header, WriteMultipleRequest wmReq, RedisValueInfo oldVal)
        throws RedisResponseException{
        
        MapValue smallVal = header.smallVal;
        header.smallVal = null;
        header.len = smallVal.size();
        
        if (header.len > MAX_TXN_ELEM_CNT - 1) {
            throw RedisResponseException.corrupt("SmallVal hash is too big");
        }

        wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);

        for(Map.Entry<String, FieldValue> ent : smallVal) {
            // The values in the map already contain key and value, we only
            // need to add slot, id, keyId and cid.
            wmReq.add(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(ent.getValue().asMap()
                    .put(FLD_SLOT, keyInfo.slot)
                    .put(FLD_ID, keyInfo.id)
                    .put(FLD_KEY_ID, ent.getKey())
                    .put(FLD_CID, header.cid)), false);
        }
    }

    private HSetResult prepareHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        String[] fKeyIds, HSetInfo hsi, WriteMultipleRequest wmReq,
        RedisValueInfo oldVal, boolean hasLargeEntry)
        throws RedisResponseException {
        // If hsi = null, we are creating new hash.
        HashHeader header = hsi != null ? hsi.header : new HashHeader();

        int addedCnt;
        int processedCnt;

        if (header.smallVal != null) {
            addedCnt = 0;
            for(processedCnt = 0; processedCnt < fKeyIds.length;
                processedCnt++) {
                if (header.smallVal.size() > MAX_SMALL_HASH_SIZE) {
                    break;
                }
                String keyId = fKeyIds[processedCnt];
                if (hsi == null || !header.smallVal.contains(keyId)) {
                    addedCnt++;
                }
                header.smallVal.put(keyId, fvMap.get(keyId));
            }

            HSetResult res = new HSetResult(addedCnt, processedCnt);

            if (hasLargeEntry ||
                header.smallVal.size() > MAX_SMALL_HASH_SIZE) {
                convertToMultiRow(keyInfo, header, wmReq, oldVal);
            } else {
                // The hash is still in smallVal format.
                wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
            }

            if (hsi == null) {
                // add the empty record
                wmReq.add(makePutEmptyReq(keyInfo, header), false);
            }

            return res;
        }

        // The hash is in multi-row format. The value must have already
        // existed (otherwise we would be using smallVal above).
        assert hsi != null;
        chkHeaderLength(header);

        // addedCnt is number of fields set (no duplicates) minus number
        // of fields already existing (set in previous callback).
        processedCnt = fKeyIds.length;
        addedCnt = processedCnt - hsi.existingCnt;
        header.len += addedCnt;

        wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
        for(int i = 0; i < fKeyIds.length; i++) {
            String keyId = fKeyIds[i];
            MapValue val = fvMap.get(keyId).asMap();
            assert val != null;

            // The values in the map already contain key and value, we
            // only need to add slot, id, keyId and cid.
            wmReq.add(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(val.put(FLD_SLOT, keyInfo.slot)
                    .put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, keyId)
                    .put(FLD_CID, header.cid)), false);
        }

        return new HSetResult(addedCnt, processedCnt);
    }

    private int doHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        boolean hasLargeEntry) throws RedisResponseException {
        String[] keyIds = fvMap.getMap().keySet().stream()
            .limit(MAX_TXN_ELEM_CNT).toArray(String[]::new);

        return doWithRetries(() -> {
            CollectionValueResult<HSetInfo> cvr = getExistingVal(keyInfo,
                keyIds, (header, rows) ->
                    new HSetInfo(header, rows != null ? rows.size() : 0));
            WriteMultipleRequest wmReq = new WriteMultipleRequest();
            HSetResult hsr = prepareHSet(keyInfo, fvMap, keyIds, cvr.data,
                wmReq, cvr.val, hasLargeEntry);

            WriteMultipleResult wmRes = doChkWM(wmReq, keyInfo, cvr.val);

            // We must have WM results either from keyIds or
            // convertToMultiRow().
            if (!wmRes.getSuccess() || wmRes.size() <= 0) {
                throw RedisResponseException.corrupt(
                    "Invalid WriteMultiple result or missing op results");
            }

            // Remove elements we already processed. There may be fewer of
            // them than elements in keyIds (if we exceeded smallVal size
            // limit and called convertToMultiRow()).
            for(int i = 0; i < hsr.processedCnt; i++) {
                fvMap.remove(keyIds[i]);
            }

            return hsr.addedCnt;
        });
    }

    private int doHandleHSet(RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        // one or more field-value pairs plus the key
        if (cmd.args.length % 2 != 1) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        // We put the fields and values into a map at first even for multi-row
        // format. This is to account for potential duplicate fields in input.
        // Redis allows duplicate fields in SET commands and just uses the
        // last value to set. We have to eliminate duplicates to calculate
        // correct added count and also because WriteMultipleRequest cannot
        // contain requests with duplicate primary keys.
        MapValue fvMap = new MapValue();
        boolean hasLargeEntry = false;

        for(int i = 1; i < cmd.args.length; i += 2) {
            ByteBuf hKeyBuf = cmd.args[i];
            ByteBuf hValBuf = cmd.args[i + 1];
            chkEntrySize(hKeyBuf, hValBuf);
            RedisKeyInfo hki = makeRedisKeyInfo(hKeyBuf);
            String val = makeStrVal(hValBuf);

            // For simplicity, we store the data in the same format in
            // smallVal as in multi-row format, having "key" and "value"
            // field for each entry. Note that the key might also store
            // per-hash-field expiration time (added to Redis 7.4) as well
            // as scanId. It is preferable to compute scanId in the same
            // way as for multi-row format so that the scan still works
            // even if the hash gets converted to multi-row format during
            // the scan.
            fvMap.put(hki.id, new MapValue()
                .put(FLD_KEY, makeRedisKey(hki)).put(FLD_VALUE, val));

            if (hki.data.length() + val.length() > MAX_SMALL_HASH_ENT_SIZE) {
                hasLargeEntry = true;
            }
        }

        int addedCnt = 0;
        while(fvMap.size() > 0) {
            addedCnt += doHSet(keyInfo, fvMap, hasLargeEntry);
        }

        return addedCnt;
    }

    private int prepareHDel(RedisKeyInfo keyInfo, String[] fKeyIds,
        HDelInfo hdi, WriteMultipleRequest wmReq, RedisValueInfo oldVal)
        throws RedisResponseException {
        assert hdi != null;

        HashHeader header = hdi.header;
        assert header != null;

        if (header.smallVal != null) {
            int delCnt = 0;
            if (header.smallVal.size() > MAX_SMALL_HASH_SIZE) {
                throw RedisResponseException.corrupt(
                    "Invalid size of smallVal entry in hash header");
            }

            for(int i = 0; i < fKeyIds.length; i++) {
                if (header.smallVal.remove(fKeyIds[i]) != null) {
                    delCnt++;
                }
            }

            if (header.smallVal.size() != 0) {
                wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
            } else {
                wmReq.add(makeDeleteKeyReq(keyInfo, oldVal), true);
                // delete the empty record
                wmReq.add(makeDeleteEmptyReq(keyInfo), false);
            }

            return delCnt;
        }

        int existingCnt = hdi.existingIds != null ? hdi.existingIds.size() : 0;
        if (existingCnt == 0) {
            // Nothing to delete.
            return 0;
        }

        header.len -= existingCnt;

        if (header.len > 0) {
            wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
        } else if (header.len == 0) {
            wmReq.add(makeDeleteKeyReq(keyInfo, oldVal), true);
            // delete the empty record
            wmReq.add(makeDeleteEmptyReq(keyInfo), false);
        } else {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }
        
        // We only add delete requests for existing fields.
        for(int i = 0; i < existingCnt; i++) {
            wmReq.add(new DeleteRequest()
                .setTableName(HASH_TABLE_NAME)
                .setKey(new MapValue().put(FLD_SLOT, keyInfo.slot)
                .put(FLD_ID, keyInfo.id)
                .put(FLD_KEY_ID, hdi.existingIds.get(i))), false);
        }

        return existingCnt;
    }

    private int doHDel(RedisKeyInfo keyInfo, Set<String> fSet)
        throws RedisResponseException {
        // Subtract 1 from MAX_TXN_ELEM_CNT to account for the empty record.
        String[] keyIds = fSet.stream().limit(MAX_TXN_ELEM_CNT - 1)
            .toArray(String[]::new);

        return doWithRetries(() -> {
            CollectionValueResult<HDelInfo> cvr = getExistingVal(keyInfo,
                keyIds, (header, rows) -> {
                if (rows == null) {
                    return new HDelInfo(header, null);
                }
                int cnt = rows.size();
                ArrayList<String> ids = new ArrayList<>(cnt);
                for(int i = 0; i < cnt; i++) {
                    ids.add(rowToKeyId(rows.get(i)));
                }
                return new HDelInfo(header, ids);
            });

            int delCnt = 0;
            WriteMultipleRequest wmReq = null;

            if (cvr.isValid()) {
                wmReq = new WriteMultipleRequest();
                delCnt = prepareHDel(keyInfo, keyIds, cvr.data, wmReq,
                    cvr.val);
            }

            if (delCnt != 0) {
                doWM(wmReq, true);
            } else {
                // Either hash doesn't exist or expired or prepareHDel() didn't
                // find any fields to delete.
                if (!cvr.val.exists()) {
                    // If the query returned no records, we still have to
                    // guard against key of wrong type (see comments for
                    // doChkWM()).
                    chkHashKey(keyInfo);
                }
                // We return -1 to indicate that the hash does not exist or
                // expired.
                if (!cvr.isValid()) {
                    return -1;
                }
            }

            // Even if prepareHDel() did not find any of the provided fields,
            // we still have to remove each field from fSet below.
            // Unlike for HSET, here all elements of keyIds should be
            // processed on successful request.
            for(int i = 0; i < keyIds.length; i++) {
                fSet.remove(keyIds[i]);
            };
            return delCnt;
        });
    }

    private CollectionValueResult<HValInfo> getExistingFldVal(RedisKeyInfo ki,
        RedisKeyInfo hki) throws RedisResponseException {
            List<MapValue> rows = doQuery(ki, SQL_ENTRY_VAL,
            new StringValue(hki.id));
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

        String fldVal;
        // The 1st record must be the empty record.
        if (rows.size() == 1) {
            // If there is only one row, then either the hash is in smallVal
            // format or it is multi-row format and the hKey is not found.
            // In either case, the only returned row should be the empty
            // record.
            if (getStringField(row0, FLD_FLD_VAL, true) != null) {
                throw RedisResponseException.corrupt(ERR_INVALID_HASH_ENTRY);
            }
            fldVal = header.smallVal != null ?
                getValFromSmallVal(header.smallVal, hki.id) : null;
        } else {
            // The hash must be in multi-row format and the hKey found.
            // There should be 2 rows: the empty record and the one with hKey.
            chkMaxNumResults(rows, 2);
            if (header.smallVal != null) {
                throw RedisResponseException.corrupt(ERR_INVALID_HASH_HEADER);
            }
            // We are not sure of the sorting order although in most cases
            // the row with hKey will follow the empty record.
            fldVal = getStringField(rows.get(1), FLD_FLD_VAL, true);
            if (fldVal == null) {
                fldVal = getStringField(row0, FLD_FLD_VAL);
            }
        }

        return new CollectionValueResult<>(val, new HValInfo(header, fldVal));
    }

    // Somewhat like prepareHSet but simpler, since we are only updating one
    // field.
    private void prepareUpdVal(RedisKeyInfo keyInfo, RedisKeyInfo hki,
        HValInfo hvi, WriteMultipleRequest wmReq, RedisValueInfo oldVal,
        ByteBuf newVal) throws RedisResponseException {
        HashHeader header = hvi != null ? hvi.header : new HashHeader();
    
        String newValStr = makeStrVal(newVal);
        MapValue mapVal = new MapValue()
            .put(FLD_KEY, makeRedisKey(hki)).put(FLD_VALUE, newValStr);
        
        if (header.smallVal != null) {
            header.smallVal.put(hki.id, mapVal);
            // Convert to multi-row format if new value is big.
            if (hki.data.length() + newValStr.length() >
                MAX_SMALL_HASH_ENT_SIZE) {
                convertToMultiRow(keyInfo, header, wmReq, oldVal);
            } else {
                // The hash is still in smallVal format.
                wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
            }

            if (hvi == null) { // hash is new
                // add the empty record
                wmReq.add(makePutEmptyReq(keyInfo, header), false);
            }

            return;
        }

        // The hash is in multi-row format. The hash must have already
        // existed. (otherwise we would be using smallVal above).
        assert hvi != null;
        chkHeaderLength(header);

        if (hvi.fldVal == null) {
            // The field was not in the hash, so we are adding new field.
            header.len++;
        }

        wmReq.add(makePutKeyReq(keyInfo, header, oldVal), true);
        // The values in the map already contain key and value, we
        // only need to add id, keyId and cid.
        wmReq.add(new PutRequest()
            .setTableName(HASH_TABLE_NAME)
            .setValue(mapVal.put(FLD_SLOT, keyInfo.slot)
                .put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, hki.id)
                .put(FLD_CID, header.cid)), false);
    }

    private RedisMessage doUpdateVal(RedisKeyInfo keyInfo,
        RedisKeyInfo hKeyInfo,
        ThrowingFunction<ByteBuf,ByteBuf,RedisResponseException> getNewVal,
        ThrowingFunction<ByteBuf,RedisMessage,RedisResponseException>
            getResult) throws RedisResponseException {
        return doWithRetries(() -> {
            CollectionValueResult<HValInfo> cvr =
                getExistingFldVal(keyInfo, hKeyInfo);
            HValInfo hvi = cvr.data;
            ByteBuf newVal = getNewVal.apply(
                hvi != null && hvi.fldVal != null ?
                    getStrVal(hvi.fldVal) : null);

            // We use newVal = null to indicate that the value should not be
            // updated. This works for existing commands, but may need to be
            // reconsidered.
            if (newVal != null) {
                WriteMultipleRequest wmReq = new WriteMultipleRequest();
                prepareUpdVal(keyInfo, hKeyInfo, hvi, wmReq, cvr.val, newVal);
                doChkWM(wmReq, keyInfo, cvr.val);
            } else if (!cvr.val.exists()) {
                // If the query returned no records, we still have to guard
                // against key of wrong type (see comments for doChkWM()).
                chkHashKey(keyInfo);
            }
            return getResult.apply(newVal);
        });
    }

    public HashUpdate(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_HSET, this::handleHSet);
        cmdMap.put(CMD_HMSET, this::handleHMSet);
        cmdMap.put(CMD_HDEL, this::handleHDel);
        cmdMap.put(CMD_HINCRBY, this::handleHIncrBy);
        cmdMap.put(CMD_HINCRBYFLOAT, this::handleHIncrByFloat);
        cmdMap.put(CMD_HSETNX, this::handleHSetNX);
    }

    public RedisMessage handleHSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return new IntegerRedisMessage(doHandleHSet(cmd));
    }

    public RedisMessage handleHMSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        doHandleHSet(cmd);
        return okReply;
    }

    public RedisMessage handleHDel(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        // We have to eliminate duplicates from the input for the same reason
        // as for HSET.
        HashSet<String> fSet = new HashSet<>();

        for(int i = 1; i < cmd.args.length; i++) {
            fSet.add(makeRedisKeyInfo(cmd.args[i]).id);
        }

        int delCnt = 0;
        while(fSet.size() > 0) {
            int cnt = doHDel(keyInfo, fSet);
            if (cnt == -1) { // hash does not exist
                break;
            }
            delCnt += cnt;
        }

        return new IntegerRedisMessage(delCnt);
    }

    public RedisMessage handleHIncrBy(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        final long arg = Utils.byteBufToLong(cmd.args[2]);
        return doUpdateVal(makeRedisKeyInfo(cmd.args[0]),
            makeRedisKeyInfo(cmd.args[1]), val -> Utils.incrBy(val, arg, true),
            newVal -> new IntegerRedisMessage(Utils.byteBufToLong(newVal)));
    }

    public RedisMessage handleHIncrByFloat(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        final double arg = Utils.byteBufToDouble(cmd.args[2]);
        return doUpdateVal(makeRedisKeyInfo(cmd.args[0]),
            makeRedisKeyInfo(cmd.args[1]),
            oldVal -> Utils.incrByFloat(oldVal, arg),
            newVal -> new FullBulkStringRedisMessage(newVal));
    }

    public RedisMessage handleHSetNX(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        chkEntrySize(cmd.args[1], cmd.args[2]);
        return doUpdateVal(makeRedisKeyInfo(cmd.args[0]),
            makeRedisKeyInfo(cmd.args[1]),
            oldVal -> oldVal == null ? cmd.args[2] : null,
            newVal -> newVal != null ? oneReply : zeroReply);        
    }

}
