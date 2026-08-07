/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.*;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

import static oracle.nosql.redis.util.Utils.*;

public class HashRead extends HashCommandsBase {

    // Here we will alias the hash key value as hashVal and keep name "value"
    // for hash entry values. This will make it easier to reuse the code
    // handling smallVal format and multi-row format as well as the Scan code.
    private static final String FLD_HASH_VAL = "hashVal";

    private static final String SEL_HASH_VAL =
        "(CASE WHEN $h.keyId = '' THEN $r.value ELSE NULL END) AS hashVal";

    // We only need value from redis main table if the hash is in smallVal
    // format, we use CASE expr. to avoid returning it otherwise.
    private static final String SQL_READ_FMT = DECL_KEY_ID +
        "%s SELECT " + SEL_HASH_VAL + "%s " + FROM_JOIN_WHERE_H_PK + "%s " +
        AND_NOT_EXPIRED;

    private static final String SEL_FLD_KEY = ", $h.key.data AS fldKey";
    private static final String SEL_FLD_VAL = ", $h.value";
    private static final String SEL_FLD_KEY_VAL = SEL_FLD_KEY + SEL_FLD_VAL;
    
    private static final String SQL_HGET_KEYVALS = String.format(SQL_READ_FMT,
        VAR2_STRING_ARRAY, ", $h.keyId" + SEL_FLD_VAL, HKEYID_IN_ARRAY_VAR2);
    private static final String SQL_HGET_VAL = String.format(SQL_READ_FMT,
        VAR2_STRING, SEL_FLD_VAL, HKEYID_EQ_VAL_VAR2);
    //private static final String SQL_HGET_EXISTS = String.format(SQL_READ_FMT,
    //    VAR2_STRING, "", HKEYID_EQ_VAL_VAR2);
    
    private static final String SQL_GET_KEYS = String.format(SQL_READ_FMT, "",
        SEL_FLD_KEY, "");
    private static final String SQL_GET_VALS = String.format(SQL_READ_FMT, "",
        SEL_FLD_VAL, "");
    private static final String SQL_GET_ALL = String.format(SQL_READ_FMT, "",
        SEL_FLD_KEY_VAL, "");
    
    private static class HashScan extends QueryScan {

        // The reason we have to use JOIN and not just query of valkey.hashes is
        // because we have to condition on cid to avoid returning obsolete
        // records that happen to have the same slot and keyId. This also
        // allows checking expiration time of parent key inside the query.
        private static final String SQL_SCAN_FMT = DECL_KEY_ID +
            " $scanId LONG; SELECT $h.key%s, " + SEL_HASH_VAL + ' ' +
            FROM_JOIN_WHERE_H_PK + AND_NOT_EXPIRED +
            "AND $h.key.scanId >= $scanId ORDER BY $h.key.scanId";
        private static final String SQL_SCAN = String.format(SQL_SCAN_FMT,
            SEL_FLD_VAL);
        private static final String SQL_SCAN_NOVAL = String.format(
            SQL_SCAN_FMT, "");

        private final boolean incVals;
        private boolean isSmallVal;

        HashScan(CommandsBase cmds, RedisKeyInfo keyInfo, long cursor,
            ByteBuf match, long count, boolean incVals)
            throws RedisResponseException {
            super(cmds, keyInfo, cursor, match, count, null);
            this.incVals = incVals;
        }

        @Override
        String getSQLScan() {
            return incVals ? SQL_SCAN : SQL_SCAN_NOVAL;
        }

        @Override
        boolean toGetAll() { return isSmallVal; }

        @Override
        Iterator<MapValue> startScan() throws RedisResponseException {
            Iterator<MapValue> iter = super.startScan();
            MapValue row0 = null;
            if (zeroCursor) { // cursor = 0
                if (!iter.hasNext()) {
                    // Check if key is wrong type.
                    chkHashKey(this, keyInfo);
                    // Hash does not exist or expired.
                    return Collections.emptyIterator();
                }

                row0 = iter.next();
                MapValue hashVal = getMapField(row0, FLD_HASH_VAL, true);
                if (hashVal != null) {
                    // Empty record should be last in the query. If we get it
                    // here, this means that this is the only record and the
                    // hash is in smallVal format.
                    if (iter.hasNext()) {
                        throw RedisResponseException.corrupt(
                            ERR_INVALID_HASH_ENTRY);
                    }
                    HashHeader header = new HashHeader(hashVal);
                    if (header.smallVal == null) {
                        throw RedisResponseException.corrupt(
                            ERR_INVALID_HASH_HEADER);
                    }

                    // For smallVal format we just return all entries. We still
                    // have to make sure each entry is a map.
                    isSmallVal = true;
                    return iterableToStream(header.smallVal.values()).map(
                        entVal -> {
                            if (!entVal.isMap()) {
                                throw RedisResponseException.unchecked(
                                    RedisResponseException.corrupt(
                                        ERR_INVALID_HASH_ENTRY));
                            }
                            return entVal.asMap();
                        }).iterator();
                }
            }

            // Since scan of smallVal will always return all the elements, we
            // can safely assume that if the cursor != 0, the hash is not in
            // smallVal format (of course if cursor != 0 is a user error, the
            // query will only fetch the empty record). But we have to exclude
            // the empty record from the original iterator. Perhaps creating
            // a wrapper iterator that discards last record (which must be the
            // empty record) would be a little more efficient than the
            // filtering of Stream below, but this should be adequate.
            Stream<MapValue> itStream = iteratorToStream(iter);
            return (row0 != null ?
                Stream.concat(Stream.of(row0), itStream) : itStream)
                .filter(row -> {
                    try {
                        return getMapField(row, FLD_HASH_VAL, true) == null;
                    } catch (RedisResponseException ex) {
                        throw RedisResponseException.unchecked(ex);
                    }
                }).iterator();
        }
        
        void addResults(List<RedisMessage> results, ByteBuf keyBuf,
            MapValue row) throws RedisResponseException {
            super.addResults(results, keyBuf, row);
            if (incVals) {
                results.add(new FullBulkStringRedisMessage(
                    getStrVal(rowToFldVal(row))));
            }
        }
    }

    private RedisMessage doHGetAll(ByteBuf keyBuf, boolean incKeys,
        boolean incVals) throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);
        List<MapValue> rows = doQuery(keyInfo, incKeys ?
            (incVals ? SQL_GET_ALL : SQL_GET_KEYS) : SQL_GET_VALS);

        if (rows.isEmpty()) {
            // Check if key is wrong type.
            chkHashKey(keyInfo);
            // The hash does not exist or expired.
            return ArrayRedisMessage.EMPTY_INSTANCE;
        }

        ArrayList<RedisMessage> res = new ArrayList<>();

        if (rows.size() == 1) {
            // The hash must be in smallVal format and the only returned row
            // should be the empty record (with non-null hashVal column)
            MapValue smallVal = valToSmallVal(
                getMapField(rows.get(0), FLD_HASH_VAL), false);
            for(FieldValue fv : smallVal.values()) {
                if (!fv.isMap()) {
                    throw RedisResponseException.corrupt(
                        ERR_INVALID_HASH_ENTRY);
                }
                MapValue mapVal = fv.asMap();
                if (incKeys) {
                    res.add(new FullBulkStringRedisMessage(
                        keyToKeyBuf(rowToKey(mapVal))));
                }
                if (incVals) {
                    res.add(new FullBulkStringRedisMessage(getStrVal(
                        getStringField(mapVal, FLD_VALUE))));
                }
            }
        } else {
            // The hash must be in multi-row format.
            int cnt = rows.size();

            // We could also add verification that the empty record is
            // present and that the number of rows corresponds to the "len"
            // field in the header (cnt == header.len + 1).
            for(int i = 1; i < cnt; i++) {
                MapValue row = rows.get(i);
                // Skip the empty record.
                if (getMapField(row, FLD_HASH_VAL, true) != null) {
                    continue;
                }
                if (incKeys) {
                    res.add(new FullBulkStringRedisMessage(getStrVal(
                        getStringField(row, FLD_FLD_KEY))));
                }
                if (incVals) {
                    res.add(new FullBulkStringRedisMessage(getStrVal(
                        getStringField(row, FLD_VALUE))));
                }
            }
        }

        return new ArrayRedisMessage(res);
    }

    private ByteBuf doHGet(ByteBuf keyBuf, ByteBuf fldBuf)
        throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);
        String hKeyId = makeRedisKeyInfo(fldBuf).id;
        String fldVal;
        List<MapValue> rows = doQuery(keyInfo, SQL_HGET_VAL,
            new StringValue(hKeyId));
        if (rows.isEmpty()) {
            // Check if key is wrong type.
            chkHashKey(keyInfo);
            // Hash does not exist or expired.
            return null;
        }

        MapValue row0 = rows.get(0);
        MapValue hashVal = getMapField(row0, FLD_HASH_VAL, true);

        if (rows.size() == 1) {
            // Hash is in smallVal format or the field is not found.
            // In either case, the only returned row should be the empty
            // record (with non-null hashVal column).
            if (hashVal == null) {
                throw RedisResponseException.corrupt(ERR_INVALID_HASH_ENTRY);
            }
            MapValue smallVal = valToSmallVal(hashVal, true);
            if (smallVal == null) {
                // The hash must be in multi-row format and the field is not
                // found.
                return null;
            }
            // The hash is in smallVal format, we get the field from smallVal.
            fldVal = getValFromSmallVal(smallVal, hKeyId);
        } else {
            // The hash must be in multi-row format and the field present.
            // Check which of the 2 rows has the field.
            chkMaxNumResults(rows, 2);
            fldVal = rowToFldVal(hashVal != null ? rows.get(1) : row0);
        }

        return fldVal != null ? getStrVal(fldVal) : null;
    }

    public HashRead(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_HLEN, this::handleHLen);
        cmdMap.put(CMD_HGET, this::handleHGet);
        cmdMap.put(CMD_HMGET, this::handleHMGet);
        cmdMap.put(CMD_HSCAN, this::handleHScan);
        cmdMap.put(CMD_HKEYS, this::handleHKeys);
        cmdMap.put(CMD_HVALS, this::handleHVals);
        cmdMap.put(CMD_HGETALL, this::handleHGetAll);
        cmdMap.put(CMD_HEXISTS, this::handleHExists);
        cmdMap.put(CMD_HSTRLEN, this::handleHStrLen);
    }

    public RedisMessage handleHLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        HashHeader header = doGetHash(makeRedisKeyInfo(cmd.args[0])).data;
        long len = header != null ?
            (header.smallVal != null ? header.smallVal.size() : header.len) :
            0;
        return new IntegerRedisMessage(len);
    }

    public RedisMessage handleHGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        ByteBuf res = doHGet(cmd.args[0], cmd.args[1]);
        return res != null ?
            new FullBulkStringRedisMessage(res) :
            FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleHMGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        // We will reuse hKeyIds for convenience.
        ArrayValue hKeyIds = new ArrayValue().addAll(
            Arrays.stream(cmd.args, 1, cmd.args.length)
                .map(uncheckedFunc(val ->
                    new StringValue(makeRedisKeyInfo(val).id))));

        List<MapValue> rows = doQuery(keyInfo, SQL_HGET_KEYVALS, hKeyIds);
        if (rows.isEmpty()) {
            // Check if key is wrong type.
            chkHashKey(keyInfo);
            // Hash does not exist or expired.
            return new ArrayRedisMessage(Collections.nCopies(hKeyIds.size(),
                FullBulkStringRedisMessage.NULL_INSTANCE));
        }

        // map and smallVal are exclusive
        HashMap<String, String> map = null;
        MapValue smallVal = null;

        // Either the hash is in smallVal format or none of the fields are
        // found.
        if (rows.size() == 1) {
            // Hash is in smallVal format or none of the fields are found.
            // In either case, the only returned row should be the empty
            // record (with non-null hashVal column).
            smallVal = valToSmallVal(
                getMapField(rows.get(0), FLD_HASH_VAL), true);
            if (smallVal == null) {
                // The hash must be in multi-row format and none of the fields
                // are found.
                return new ArrayRedisMessage(Collections.nCopies(
                    hKeyIds.size(),
                    FullBulkStringRedisMessage.NULL_INSTANCE));
            }
        } else {
            // The hash is in multi-row format and at least some fields
            // present. We collect the field values into a map to retrieve
            // them later when computing the result (which has to return a
            // value or nil for each input field in the order of the input
            // fields). The empty record is skipped.
            chkMaxNumResults(rows, hKeyIds.size() + 1);
            int cnt = rows.size();
            map = new HashMap<>(cnt - 1);
            // We could also add verification that there is one and only one
            // empty record in the results.
            for(int i = 1; i < cnt; i++) {
                MapValue row = rows.get(i);
                // Skip the empty record.
                if (getMapField(row, FLD_HASH_VAL, true) != null) {
                    continue;
                }
                map.put(rowToKeyId(row), rowToFldVal(row));
            }
        }

        ArrayList<RedisMessage> res = new ArrayList<>();
        int cnt = hKeyIds.size();
        // For each field provided, we return the value if found, or null.
        for(int i = 0; i < cnt; i++) {
            String keyId = hKeyIds.get(i).getString();
            String fldVal = smallVal != null ?
                getValFromSmallVal(smallVal, keyId) : map.get(keyId);
            res.add(fldVal != null ?
                new FullBulkStringRedisMessage(getStrVal(fldVal)) :
                FullBulkStringRedisMessage.NULL_INSTANCE);
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleHScan(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        chkMinNumArgs(cmd, 2);
        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        
        long cursor;
        try {
            cursor = Utils.byteBufToLong(cmd.args[1]);
        } catch (RedisResponseException ex) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "invalid cursor");
        }

        ByteBuf match = null;
        long count = Scan.DEFAULT_COUNT;
        boolean noVals = false;

        for(int i = 2; i < cmd.args.length; i++) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("MATCH")) {
                match = cmd.args[++i];
            } else if (arg.equalsIgnoreCase("COUNT")) {
                count = Utils.byteBufToLong(cmd.args[++i]);
                if (count <= 0) {
                    throw RedisResponseException.syntaxError();
                }
            } else if (arg.equalsIgnoreCase("NOVALUES")) {
                noVals = true;
            } else {
                throw RedisResponseException.syntaxError();
            }
        }

        try(HashScan scan = new HashScan(this, keyInfo, cursor, match, count,
            !noVals)) {
            return scan.scan();
        }
    }

    public RedisMessage handleHKeys(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doHGetAll(cmd.args[0], true, false);
    }

    public RedisMessage handleHVals(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doHGetAll(cmd.args[0], false, true);
    }

    public RedisMessage handleHGetAll(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return doHGetAll(cmd.args[0], true, true);
    }

    // This needs to be optimized using SQL_HGET_EXISTS query to avoid
    // retrieving the whole value.
    public RedisMessage handleHExists(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        ByteBuf res = doHGet(cmd.args[0], cmd.args[1]);
        return res != null ? oneReply : zeroReply;
    }

    // This can also be optimized by computing string length on the server
    // instead of retrieving the whole value. This is not easy however, since
    // the value can be either text or base64-encoded. We need to check which
    // one (by checking the first character of the value) and if base64, we
    // would also need to return the last 4 characters to check the length of
    // the padding. This might still be more efficient than retrieving the
    // whole value for large values.
    public RedisMessage handleHStrLen(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        ByteBuf res = doHGet(cmd.args[0], cmd.args[1]);
        return res != null ? new IntegerRedisMessage(res.readableBytes()) :
            zeroReply;
    }

}
