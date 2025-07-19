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
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
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

    private static void chkHeaderLength(HashHeader header)
        throws RedisResponseException{
        if (header.len <= 0) {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }
    }

    private void convertToMultiRow(RedisKeyInfo keyInfo,
        HashHeader header, CollectionUpdateInfo upInfo)
        throws RedisResponseException{
        
        MapValue smallVal = header.smallVal;
        header.smallVal = null;
        header.len = smallVal.size();
        
        if (header.len > MAX_TXN_ELEM_CNT - 1) {
            throw RedisResponseException.corrupt("SmallVal hash is too big");
        }

        upInfo.addPutKeyReq(header);

        for(Map.Entry<String, FieldValue> ent : smallVal) {
            // The values in the map already contain key and value, we only
            // need to add slot, id, keyId and cid.
            upInfo.addElemReq(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(ent.getValue().asMap()
                    .put(FLD_SLOT, keyInfo.slot)
                    .put(FLD_ID, keyInfo.id)
                    .put(FLD_KEY_ID, ent.getKey())
                    .put(FLD_CID, header.cid)));
        }
    }

    private HSetResult prepareHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        String[] fKeyIds, HSetInfo hsi, CollectionUpdateInfo upInfo,
        boolean hasLargeEntry) throws RedisResponseException {
        HashHeader header = hsi != null ? hsi.header : null;
        boolean isNew = false;

        if (header == null) {
            header = new HashHeader();
            isNew = true;
        }

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
                if (isNew || !header.smallVal.contains(keyId)) {
                    addedCnt++;
                }
                header.smallVal.put(keyId, fvMap.get(keyId));
            }

            HSetResult res = new HSetResult(addedCnt, processedCnt);

            if (hasLargeEntry ||
                header.smallVal.size() > MAX_SMALL_HASH_SIZE) {
                convertToMultiRow(keyInfo, header, upInfo);
            } else {
                // The hash is still in smallVal format.
                upInfo.addPutKeyReq(header);
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
        
        upInfo.addPutKeyReq(header);
        for(int i = 0; i < fKeyIds.length; i++) {
            String keyId = fKeyIds[i];
            MapValue val = fvMap.get(keyId).asMap();
            assert val != null;

            // The values in the map already contain key and value, we
            // only need to add slot, id, keyId and cid.
            upInfo.addElemReq(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(val.put(FLD_SLOT, keyInfo.slot)
                    .put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, keyId)
                    .put(FLD_CID, header.cid)));
        }

        return new HSetResult(addedCnt, processedCnt);
    }

    private int doHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        boolean hasLargeEntry) throws RedisResponseException {
        String[] keyIds = fvMap.getMap().keySet().stream()
            .limit(MAX_TXN_ELEM_CNT).toArray(String[]::new);

        return doMultiUpdate(keyInfo,
            ki -> getExistingVal(ki, keyIds, (header, rows) ->
                new HSetInfo(header, rows != null ? rows.size() : 0)),
            (ki, hsi, upInfo) -> prepareHSet(ki, fvMap, keyIds, hsi, upInfo,
                hasLargeEntry),
            (hsr, wmRes) -> {
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

    private int prepareHDel(RedisKeyInfo keyInfo, String[] fKeyIds,
        HDelInfo hdi, CollectionUpdateInfo upInfo)
        throws RedisResponseException {
        if (hdi == null) {
            // Hash does not exist.
            return 0;
        }

        HashHeader header = hdi.header;
        assert header != null;

        if (header.smallVal != null) {
            int delCnt = 0;
            assert header.smallVal.size() < MAX_SMALL_HASH_SIZE;

            for(int i = 0; i < fKeyIds.length; i++) {
                if (header.smallVal.remove(fKeyIds[i]) != null) {
                    delCnt++;
                }
            }

            if (header.smallVal.size() != 0) {
                upInfo.addPutKeyReq(header);    
            } else {
                upInfo.addDeleteKeyReq();
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
            upInfo.addPutKeyReq(header);
        } else if (header.len == 0) {
            upInfo.addDeleteKeyReq();
        } else {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }
        
        // We only add delete requests for existing fields.
        for(int i = 0; i < existingCnt; i++) {
            upInfo.addElemReq(new DeleteRequest()
                .setTableName(HASH_TABLE_NAME)
                .setKey(new MapValue().put(FLD_SLOT, keyInfo.slot)
                .put(FLD_ID, keyInfo.id)
                .put(FLD_KEY_ID, hdi.existingIds.get(i))));
        }

        return existingCnt;
    }

    private int doHDel(RedisKeyInfo keyInfo, Set<String> fSet)
        throws RedisResponseException {
        String[] keyIds = fSet.stream().limit(MAX_TXN_ELEM_CNT)
            .toArray(String[]::new);

        return doMultiUpdate(keyInfo,
            ki -> getExistingVal(ki, keyIds, (header, rows) -> {
                if (rows == null) {
                    return new HDelInfo(header, null);
                }
                int cnt = rows.size();
                ArrayList<String> ids = new ArrayList<>(cnt);
                for(int i = 0; i < cnt; i++) {
                    ids.add(rowToKeyId(rows.get(i), false));
                }
                return new HDelInfo(header, ids);
            }),
            (ki, hdi, upInfo) -> prepareHDel(keyInfo, keyIds, hdi, upInfo),
            (delCnt, wmRes) -> {
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
        chkSingleResult(rows);
        MapValue row0 = rows.get(0);
        RedisValueInfo val = new RedisValueInfo(rowToValue(row0),
            rowToVer(row0), getExpTime(rowToKey(row0)));
        HashHeader header = new HashHeader(val.val);
        String fldVal = getStringField(row0, FLD_FLD_VAL, true);
        if (fldVal == null && header.smallVal != null) {
            fldVal = getValFromSmallVal(header.smallVal, hki.id);
        }
        return new CollectionValueResult<>(val,
            new HValInfo(header, fldVal));
    }

    // Somewhat like prepareHSet but simpler, since we are only updating one
    // field.
    private void prepareUpdVal(RedisKeyInfo keyInfo, RedisKeyInfo hki,
        HValInfo hvi, CollectionUpdateInfo upInfo, ByteBuf newVal)
        throws RedisResponseException {
        HashHeader header = hvi != null ? hvi.header : new HashHeader();
    
        String newValStr = makeStrVal(newVal);
        MapValue mapVal = new MapValue()
            .put(FLD_KEY, makeRedisKey(hki)).put(FLD_VALUE, newValStr);
        
        if (header.smallVal != null) {
            header.smallVal.put(hki.id, mapVal);
            // Convert to multi-row format if new value is big.
            if (hki.data.length() + newValStr.length() >
                MAX_SMALL_HASH_ENT_SIZE) {
                convertToMultiRow(keyInfo, header, upInfo);
            } else {
                // The hash is still in smallVal format.
                upInfo.addPutKeyReq(header);
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
        
        upInfo.addPutKeyReq(header);
        // The values in the map already contain key and value, we
        // only need to add id, keyId and cid.
        upInfo.addElemReq(new PutRequest()
            .setTableName(HASH_TABLE_NAME)
            .setValue(mapVal.put(FLD_SLOT, keyInfo.slot)
                .put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, hki.id)
                .put(FLD_CID, header.cid)));
    }

    private RedisMessage doUpdateVal(RedisKeyInfo keyInfo, RedisKeyInfo hki,
        ThrowingFunction<ByteBuf,ByteBuf,RedisResponseException> getNewVal,
        ThrowingFunction<ByteBuf,RedisMessage,RedisResponseException>
            getResult) throws RedisResponseException {
        return doMultiUpdate(keyInfo, ki -> getExistingFldVal(ki, hki),
            (ki, hvi, upInfo) -> {
            ByteBuf newVal = getNewVal.apply(
                hvi != null && hvi.fldVal != null ?
                    getStrVal(hvi.fldVal) : null);
            // We use newVal = null to indicate that the value should not be
            // updated. This works for existing commands, but may need to be
            // reconsidered.
            if (newVal != null) {
                prepareUpdVal(ki, hki, hvi, upInfo, newVal);
            }
            return newVal;
        },
        (newVal, wmRes) -> getResult.apply(newVal));
    }

    public HashUpdate(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_HSET, this::handleHSet);
        cmdMap.put(CMD_HDEL, this::handleHDel);
        cmdMap.put(CMD_HINCRBY, this::handleHIncrBy);
        cmdMap.put(CMD_HINCRBYFLOAT, this::handleHIncrByFloat);
        cmdMap.put(CMD_HSETNX, this::handleHSetNX);
    }

    public RedisMessage handleHSet(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 3);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        // one or more field-value pairs plus the key
        if (cmd.args.length % 2 != 1) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        // We put the fields and values into a map at first even for multi-row
        // format. This is to account for potential duplicate fields in input.
        // Redis allows duplicate fields in SET commands and just uses the
        // last value to set. We have to elimitate duplicates to calculate
        // correct added count and also because WriteMultipleRequest cannot
        // contain requests with duplicate primary keys.
        MapValue fvMap = new MapValue();
        boolean hasLargeEntry = false;

        for(int i = 1; i < cmd.args.length; i += 2) {
            RedisKeyInfo hki = makeRedisKeyInfo(cmd.args[i]);
            String val = makeStrVal(cmd.args[i + 1]);
            
            // For simplicity, we store the data in the same format in
            // smallVal as in multi-row format, having "key" and "value"
            // field for each entry. Note that the key might also store
            // per-hash-field expiration time (added to Redis 7.4) as well
            // as scanId. It is preferable to compute scanId in the same
            // way as for multi-row format so that the scan still works
            // even if the hash gets converted to mutli-row format during
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

        return new IntegerRedisMessage(addedCnt);
    }

    public RedisMessage handleHDel(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);

        // We have to elimitate duplicates from the input for the same reason
        // as for HSET.
        HashSet<String> fSet = new HashSet<>();

        for(int i = 1; i < cmd.args.length; i++) {
            fSet.add(makeRedisKeyInfo(cmd.args[i]).id);
        }

        int delCnt = 0;
        while(fSet.size() > 0) {
            delCnt += doHDel(keyInfo, fSet);
        }

        return new IntegerRedisMessage(delCnt);
    }

    public RedisMessage handleHIncrBy(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        final long arg = Utils.byteBufToLong(cmd.args[2]);
        return doUpdateVal(makeRedisKeyInfo(cmd.args[0]),
            makeRedisKeyInfo(cmd.args[1]), oldVal -> Utils.longToByteBuf(
                (oldVal != null ? Utils.byteBufToLong(oldVal) : 0) + arg),
                newVal -> new IntegerRedisMessage(
                    Utils.byteBufToLong(newVal)));
    }

    public RedisMessage handleHIncrByFloat(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        final double arg = Utils.byteBufToDouble(cmd.args[2]);
        return doUpdateVal(makeRedisKeyInfo(cmd.args[0]),
            makeRedisKeyInfo(cmd.args[1]), oldVal -> Utils.doubleToByteBuf(
                (oldVal != null ? Utils.byteBufToDouble(oldVal) : 0) + arg),
                newVal -> new FullBulkStringRedisMessage(newVal));
    }

    public RedisMessage handleHSetNX(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        return doUpdateVal(makeRedisKeyInfo(cmd.args[0]),
            makeRedisKeyInfo(cmd.args[1]),
            oldVal -> oldVal == null ? cmd.args[2] : null,
            newVal -> newVal != null ? oneReply : zeroReply);        
    }

}
