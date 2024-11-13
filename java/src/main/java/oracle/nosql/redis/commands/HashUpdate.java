package oracle.nosql.redis.commands;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;

public class HashUpdate extends HashCommandsBase {
    
    private static class HSetResult {
        final int addedCnt;
        final int processedCnt;

        HSetResult(int addedCnt, int processedCnt) {
            this.addedCnt = addedCnt;
            this.processedCnt = processedCnt;
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
            // need to add id, keyId and cid.
            upInfo.addElemReq(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(ent.getValue().asMap()
                    .put(FLD_ID, keyInfo.id)
                    .put(FLD_KEY_ID, ent.getKey())
                    .put(FLD_CID, header.cid)));
        }
    }

    private HSetResult prepareHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        String[] fKeyIds, HashValueInfo hvi, CollectionUpdateInfo upInfo,
        boolean hasLargeEntry) throws RedisResponseException {
        HashHeader header = hvi != null ? hvi.header : null;
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
        assert hvi != null;

        if (header.len <= 0) {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }

        // addedCnt is number of fields set (no duplicates) minus number
        // of fields already existing (set in previous callback).
        processedCnt = fKeyIds.length;
        addedCnt = processedCnt - hvi.existingCnt;
        header.len += addedCnt;
        
        upInfo.addPutKeyReq(header);
        for(int i = 0; i < fKeyIds.length; i++) {
            String keyId = fKeyIds[i];
            MapValue val = fvMap.get(keyId).asMap();
            assert val != null;

            // The values in the map already contain key and value, we
            // only need to add id, keyId and cid.
            upInfo.addElemReq(new PutRequest()
                .setTableName(HASH_TABLE_NAME)
                .setValue(val.put(FLD_ID, keyInfo.id).put(FLD_KEY_ID, keyId)
                    .put(FLD_CID, header.cid)));
        }

        return new HSetResult(addedCnt, processedCnt);
    }

    private int doHSet(RedisKeyInfo keyInfo, MapValue fvMap,
        boolean hasLargeEntry) throws RedisResponseException {
        String[] keyIds = fvMap.getMap().keySet().stream()
            .limit(MAX_TXN_ELEM_CNT).toArray(String[]::new);

        return doMultiUpdate(keyInfo,
            ki -> getExistingVal(ki, keyIds, false),
            (ki, hvi, upInfo) -> prepareHSet(ki, fvMap, keyIds, hvi, upInfo,
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
        HashValueInfo hvi, CollectionUpdateInfo upInfo)
        throws RedisResponseException {
        if (hvi == null) {
            // Hash does not exist.
            return 0;
        }

        HashHeader header = hvi.header;
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

        if (hvi.existingCnt == 0) {
            // Nothing to delete.
            return 0;
        }

        assert hvi.existingIds != null;
        assert hvi.existingIds.size() == hvi.existingCnt;
        header.len -= hvi.existingCnt;

        if (header.len > 0) {
            upInfo.addPutKeyReq(header);
        } else if (header.len == 0) {
            upInfo.addDeleteKeyReq();
        } else {
            throw RedisResponseException.corrupt(
                "Invalid hash header len field");
        }
        
        // We only add delete requests for existing fields.
        for(int i = 0; i < hvi.existingCnt; i++) {
            upInfo.addElemReq(new DeleteRequest()
                .setTableName(HASH_TABLE_NAME)
                .setKey(new MapValue().put(FLD_ID, keyInfo.id)
                .put(FLD_KEY_ID, hvi.existingIds.get(i))));
        }

        return hvi.existingCnt;
    }

    private int doHDel(RedisKeyInfo keyInfo, Set<String> fSet)
        throws RedisResponseException {
        String[] keyIds = fSet.stream().limit(MAX_TXN_ELEM_CNT)
            .toArray(String[]::new);

        return doMultiUpdate(keyInfo,
            ki -> getExistingVal(ki, keyIds, true),
            (ki, hvi, upInfo) -> prepareHDel(keyInfo, keyIds, hvi, upInfo),
            (delCnt, wmRes) -> {
                // Unlike for HSET, here all elements of keyIds should be
                // processed on successful request.
                for(int i = 0; i < keyIds.length; i++) {
                    fSet.remove(keyIds[i]);
                };
                return delCnt;
            });
    }

    public HashUpdate(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_HSET, this::handleHSet);
        cmdMap.put(CMD_HDEL, this::handleHDel);
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

}
