package oracle.nosql.redis.commands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class HashRead extends HashCommandsBase {

    // Here we will alias the hash key value as hashVal and keep name "value"
    // for hash entry values. This will make it easier to reuse the code
    // handling smallVal format and multi-row format as well as the Scan code.
    private static final String FLD_HASH_VAL = "hashVal";

    // We only need value from redis main table if the hash is in smallVal
    // format, we use CASE expr. to avoid returning it otherwise. Same for
    // scan.
    private static final String SQL_READ_FMT =
        "DECLARE $var1 STRING;%s SELECT (CASE WHEN $h.keyId IS NULL " +
        "THEN $r.value ELSE NULL END) AS hashVal%s " + FROM_LOJ +
        " WHERE $r.id = $var1 " + NOT_EXPIRED + "%s";

    private static final String SEL_FLD_KEY = ", $h.key.data AS fldKey";
    private static final String SEL_FLD_VAL = ", $h.value";
    private static final String SEL_FLD_KEY_VAL = SEL_FLD_KEY + SEL_FLD_VAL;
    
    private static final String SQL_HGET_KEYVALS = String.format(SQL_READ_FMT,
        VAR2_STRING_ARRAY, ", $h.keyId" + SEL_FLD_VAL, HKEYID_IN_ARRAY_VAR2);
    private static final String SQL_HGET_VAL = String.format(SQL_READ_FMT,
        VAR2_STRING, SEL_FLD_VAL, HKEYID_EQ_VAL_VAR2);
    //private static final String SQL_HGET_EXISTS = String.format(SQL_READ_FMT,
        //VAR2_STRING, "", HKEYID_EQ_VAL_VAR2);
    
    private static final String SQL_GET_KEYS = String.format(SQL_READ_FMT, "",
        SEL_FLD_KEY, "");
    private static final String SQL_GET_VALS = String.format(SQL_READ_FMT, "",
        SEL_FLD_VAL, "");
    private static final String SQL_GET_ALL = String.format(SQL_READ_FMT, "",
        SEL_FLD_KEY_VAL, "");
    
    private static class HashScan extends QueryScan {

        private static final String SQL_SCAN_FMT = String.format(SQL_READ_FMT,
            " $var2 LONG;", ", $h.key%s",
            "AND ($h.keyId IS NULL OR $h.key.scanId >= $var2) " +
            "ORDER BY $h.key.scanId");
        private static final String SQL_SCAN = String.format(SQL_SCAN_FMT,
            SEL_FLD_VAL);
        private static final String SQL_SCAN_NOVAL = String.format(
            SQL_SCAN_FMT, "");

        private final boolean incVals;

        HashScan(CommandsBase cmds, boolean incVals) {
            super(cmds);
            this.incVals = incVals;
        }

        String getSQLScan() {
            return incVals ? SQL_SCAN : SQL_SCAN_NOVAL;
        }
    
        Iterable<MapValue> scanIterable(RedisKeyInfo keyInfo, long cursor,
            int limit) throws RedisResponseException {
            Iterable<MapValue> qIter = super.scanIterable(keyInfo, cursor,
                limit);
            Iterator<MapValue> it = qIter.iterator();

            // Hash does not exist or we finished scan.
            if (!it.hasNext()) {
                return qIter;
            }

            MapValue row = it.next();
            MapValue val = getMapField(row, FLD_HASH_VAL, true);
            if (val != null) {
                // The hash must be in smallVal format and we must have
                // no more rows.
                if (it.hasNext()) {
                    throw RedisResponseException.corrupt(ERR_NO_SINGLE_RES);
                }
                MapValue smallVal = valToSmallVal(val);
                // We must only return entries >= cursor and sort them based
                // on the scanId.
                ArrayList<MapValue> vals = new ArrayList<>(val.size());
                for(FieldValue entVal : smallVal.values()) {
                    if (!entVal.isMap()) {
                        throw RedisResponseException.corrupt(
                                ERR_INVALID_SMALLVAL_ENTRY);
                    }
                    MapValue mapVal = entVal.asMap();
                    long scanId = getScanId(rowToKey(mapVal));
                    if (scanId >= cursor) {
                        vals.add(mapVal);
                    }
                }

                vals.sort((val1, val2) -> {
                    // We don't need to check val1, val2 for correctness since
                    // it is already done above.
                    return Long.compare(rowToScanIdUnchecked(val1),
                        rowToScanIdUnchecked(val1));
                });
                return vals;
            }

            // Return iterable that includes the row we already fetch followed
            // by the rest of the rows.
            return () -> Stream.concat(Stream.of(row), StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    it, Spliterator.ORDERED), false)).iterator();
        }
        
        void addResults(List<RedisMessage> results, ByteBuf keyBuf,
            MapValue row) throws RedisResponseException {
            super.addResults(results, keyBuf, row);
            if (incVals) {
                results.add(new FullBulkStringRedisMessage(
                    getStrVal(rowToFldVal(row, false))));
            }
        }
    }

    private static MapValue rowToSmallVal(MapValue row)
        throws RedisResponseException {
        return valToSmallVal(getMapField(row, FLD_HASH_VAL));
    }

    private RedisMessage doHGetAll(ByteBuf keyBuf, boolean incKeys,
        boolean incVals) throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);
        List<MapValue> rows = doQuery(keyInfo, incKeys ?
            (incVals ? SQL_GET_ALL : SQL_GET_KEYS) : SQL_GET_VALS);

        if (rows.isEmpty()) {
            // The hash does not exist.
            return ArrayRedisMessage.EMPTY_INSTANCE;
        }

        ArrayList<RedisMessage> res = new ArrayList<>();
        MapValue row0 = rows.get(0);
        MapValue val = getMapField(row0, FLD_HASH_VAL, true);
        if (val != null) {
            // The hash is in smallVal format.
            chkSingleResult(rows);
            MapValue smallVal = valToSmallVal(val);
            for(FieldValue fv : smallVal.values()) {
                if (!fv.isMap()) {
                    throw RedisResponseException.corrupt(
                        ERR_INVALID_SMALLVAL_ENTRY);
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
            // The hash is in multi-row format.
            for(MapValue row : rows) {
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
        String keyId = makeRedisKeyInfo(fldBuf).id;
        List<MapValue> rows = doQuery(makeRedisKeyInfo(keyBuf),
            SQL_HGET_VAL, new StringValue(keyId));
        if (rows.size() > 1) {
            throw RedisResponseException.corrupt(
                "Expected at most a single result, got multiple");
        }

        if (rows.size() == 0) {
            // Hash does not exist or field is not found.
            return null;
        }

        MapValue row0 = rows.get(0);
        String val = rowToFldVal(row0, true);
        if (val != null) {
            return getStrVal(val);
        }

        // Hash must be in smallVal format. Note that for HGET and HMGET we
        // put the matching field condition in the WHERE clause (and not in
        // LOJ) since we don't need to distinguish between the cases where
        // hash does not exist or matching field(s) are not found (for HSET
        // we do need to distinguish these), so empty result is returned also
        // when there are no matching fields.
        val = getValFromSmallVal(rowToSmallVal(row0), keyId);
        return val != null ? getStrVal(val) : null;
    }

    public HashRead(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
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
        // We will reuse keyIds for convenience.
        ArrayValue keyIds = new ArrayValue().addAll(
            Arrays.stream(cmd.args, 1, cmd.args.length)
                .map(val -> new StringValue(makeRedisKeyInfo(val).id)));

        List<MapValue> rows = doQuery(makeRedisKeyInfo(cmd.args[0]),
            SQL_HGET_KEYVALS, keyIds);

        if (rows.isEmpty()) {
            // Hash does not exist or hash is in multi-row format and no
            // input fields found.
            return new ArrayRedisMessage(Collections.nCopies(keyIds.size(),
                FullBulkStringRedisMessage.NULL_INSTANCE));
        }

        // map and smallVal are exclusive
        HashMap<String, String> map = null;
        MapValue smallVal = null;

        MapValue row0 = rows.get(0);
        String keyId0 = rowToKeyId(row0, true);

        if (keyId0 == null) {
            chkSingleResult(rows);
            smallVal = rowToSmallVal(row0);
        } else {
            // The hash is in multi-row format. We collect the field values
            // into a map to retrieve them later when computing the result
            // (which has to return a value or nil for each input field in
            // the order of the input fields).
            int cnt = rows.size();
            map = new HashMap<>(cnt);
            map.put(keyId0, rowToFldVal(row0, false));
            for(int i = 0; i < cnt; i++) {
                map.put(rowToKeyId(row0, false),
                    rowToFldVal(row0, false));
            }
        }

        ArrayList<RedisMessage> res = new ArrayList<>();
        int cnt = keyIds.size();
        // For each field provided, we return the value if found, or null.
        for(int i = 0; i < cnt; i++) {
            String keyId = keyIds.get(i).getString();
            String val = smallVal != null ?
                getValFromSmallVal(smallVal, keyId) : map.get(keyId);
            res.add(val != null ?
                new FullBulkStringRedisMessage(getStrVal(val)) :
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

        try(HashScan scan = new HashScan(this, !noVals)) {
            return scan.scan(keyInfo, cursor, match, count, null);
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
