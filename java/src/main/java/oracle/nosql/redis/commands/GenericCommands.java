/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.LongBinaryOperator;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.*;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.*;
import oracle.nosql.redis.CommandHandlers;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.NoSQLRedisServer;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingPredicate;
import oracle.nosql.redis.util.Utils.RedisRetryException;

import static oracle.nosql.redis.util.Utils.*;

public class GenericCommands extends CommandsBase {

    static enum TTLOpt {
        NX,
        XX,
        GT,
        LT
    }

    public static final String CMD_COPY = "COPY";
    public static final String CMD_DEL = "DEL";
    public static final String CMD_EXISTS = "EXISTS";
    public static final String CMD_PEXIPRETIME = "PEXPIRETIME";
    public static final String CMD_EXPIRETIME = "EXPIRETIME";
    public static final String CMD_PTTL = "PTTL";
    public static final String CMD_TTL = "TTL";
    public static final String CMD_PEXIPRE = "PEXPIRE";
    public static final String CMD_PEXIPREAT = "PEXPIREAT";
    public static final String CMD_EXIPRE = "EXPIRE";
    public static final String CMD_EXIPREAT = "EXPIREAT";
    public static final String CMD_PERSIST = "PERSIST";
    public static final String CMD_TYPE = "TYPE";
    public static final String CMD_SCAN = "SCAN";
    public static final String CMD_KEYS = "KEYS";
    public static final String CMD_RENAME = "RENAME";
    public static final String CMD_RENAMENX = "RENAMENX";

    private static class KeyScan extends QueryScan {

        private static final String SQL_SCAN =
            "DECLARE $scanId LONG; SELECT $r.key, $r.value.type AS type " +
            "FROM redis $r WHERE $r.key.scanId >= $scanId " + AND_NOT_EXPIRED +
            "ORDER BY $r.key.scanId";

        KeyScan(CommandsBase cmds, long cursor, ByteBuf match, long count,
            String type) throws RedisResponseException {
            super(cmds, null, cursor, match, count, typePred(type));
        }

        private static ThrowingPredicate<MapValue,RedisResponseException>
            typePred(String type) {
            return type != null ?
                row -> type.equalsIgnoreCase(getStringField(row, VALUE_TYPE)) :
                null;
        }
    
        String getSQLScan() { return SQL_SCAN; }
    }

    private static final String SQL_EXISTS_ONE = DECL_KEY_ID +
        "SELECT 1 FROM redis $r" + WHERE_KEY_ID_COND + AND_NOT_EXPIRED;
    private static final String SQL_EXISTS = DECL_KEY_IDS +
        "SELECT COUNT(*) AS res FROM redis $r " + WHERE_KEY_IDS_COND +
        AND_NOT_EXPIRED;

    private final CommandHandlers cmdHandlers;

    private static int compExp(long exp1, long exp2) {
        assert exp1 >= NO_EXP;
        assert exp2 >= NO_EXP;
        if (exp1 == NO_EXP) {
            return exp2 == NO_EXP ? 0 : 1;
        }
        if (exp2 == NO_EXP) {
            return -1;
        }

        return Long.compare(exp1, exp2);
    }

    // Used for EXPIRETIME, PXPIRETIME, TTL, PTTL.
    private RedisMessage doGetExp(RawCommand cmd, LongBinaryOperator conv)
        throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        RedisValueInfo valInfo = doGet(cmd.args[0]);
        return new IntegerRedisMessage(valInfo.isValid() ?
            (valInfo.exp == NO_EXP ? -1 : conv.applyAsLong(valInfo.exp,
                System.currentTimeMillis())) : -2);
    }

    // For commands that set expiration time, such as
    // EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST.
    private RedisMessage doSetExp(ByteBuf keyBuf, long exp, TTLOpt ttlOpt)
        throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(keyBuf);

        return doWithRetries(() -> {
            RedisValueInfo oldVal = doGet(keyInfo);
            if (!oldVal.isValid() ||
                (ttlOpt != null &&
                    (ttlOpt == TTLOpt.NX && oldVal.exp != NO_EXP) ||
                    (ttlOpt == TTLOpt.XX && oldVal.exp == NO_EXP) ||
                    (ttlOpt == TTLOpt.GT && compExp(exp, oldVal.exp) <= 0) ||
                    (ttlOpt == TTLOpt.LT && compExp(exp, oldVal.exp) >= 0))) {
                return zeroReply;
            }
            RedisValueInfo newVal = RedisValueInfo.create(oldVal.val, null,
                exp);
            doChkSet(keyInfo, newVal, oldVal);
            CommandsBase cmds = cmdHandlers.getCommandsByValueType(
                getValueType(newVal.val));
            if (cmds.isCollectionType()) {
                cmds.doSetElemsExp(keyInfo, newVal,
                    TimeToLive.fromExpirationTime(exp,
                        System.currentTimeMillis()));
            }
            return oneReply;
        });
    }

    // For all commands as above except PERSIST.
    private RedisMessage doSetExp(RawCommand cmd, TTLMode ttlMode)
        throws RedisResponseException {
        // First 2 args are key and exp time.
        chkMinNumArgs(cmd, 2);
        TTLOpt ttlOpt = null;

        for(int i = 2; i < cmd.args.length; i++) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("NX")) {
                chkNotSet(ttlOpt);
                ttlOpt = TTLOpt.NX;
            } else if (arg.equalsIgnoreCase("XX")) {
                chkNotSet(ttlOpt);
                ttlOpt = TTLOpt.XX;
            } else if (arg.equalsIgnoreCase("GT")) {
                chkNotSet(ttlOpt);
                ttlOpt = TTLOpt.GT;
            } else if (arg.equalsIgnoreCase("LT")) {
                chkNotSet(ttlOpt);
                ttlOpt = TTLOpt.LT;
            } else {
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "Unsupported option " + arg);
            }
        }

        return doSetExp(cmd.args[0], makeExpTime(cmd.args[1], ttlMode),
            ttlOpt);
    }



    // For collections, this will delete collection elements after a key is
    // deleted (see cmds.afterDelete()). Note that we do not need to worry
    // about atomicity for these collection elements. If the collection key
    // gets concurrently re-created, it will use different cid, which will not
    // clash with the elements with old cid.
    private int doDel(ByteBuf [] keys, int off, int cnt)
        throws RedisResponseException {
        RedisKeyInfo[] keyInfos = makeRedisMultiKeyInfo(keys, off, cnt);

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        HashSet<String> chkDupSet = new HashSet<>();
        for(int i = 0; i < keyInfos.length; i++) {
            RedisKeyInfo ki = keyInfos[i];
            if (!chkDupSet.add(ki.id)) {
                continue;
            }
            MapValue pk = new MapValue().put(FLD_SLOT, ki.slot)
                .put(FLD_ID, ki.id);
            DeleteRequest delReq = new DeleteRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setKey(pk).setReturnRow(true);
            wmReq.add(delReq, true);
        }
        
        WriteMultipleResult wmRes;
        try {
            wmRes = nosqlHandle.writeMultiple(wmReq);
            if (!wmRes.getSuccess()) {
                throw RedisResponseException.nosql(ERR_WM_FAIL);
            }
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }

        List<WriteMultipleResult.OperationResult> ls = wmRes.getResults();
        if (ls.size() != chkDupSet.size()) {
            throw RedisResponseException.nosql(String.format(
                "Mismatched result count from writeMultiple, " +
                "expected %d, got %d", ls.size(), keyInfos.length));
        }

        int delCnt = 0;

        // Todo: perhaps consider eliminating duplicates from the input "keys"
        // array via Stream.distinct() instead of using chkDupSet twice.
        chkDupSet.clear();
        for(int i = 0; i < keyInfos.length; i++) {
            if (!chkDupSet.add(keyInfos[i].id)) {
                continue;
            }
            WriteMultipleResult.OperationResult opRes = ls.get(i);
            if (!opRes.getSuccess()) {
                continue;
            }

            MapValue row = opRes.getExistingValue();
            if (row == null) {
                throw RedisResponseException.nosql(ERR_MISSING_EXISTING_VAL);
            }

            RedisValueInfo valInfo = RedisValueInfo.create(rowToValue(row),
                opRes.getVersion(), getExpTime(rowToKey(row)));
            if (!valInfo.isValid()) {
                continue;
            }

            ++delCnt;
            CommandsBase cmds = cmdHandlers.getCommandsByValueType(
                getValueType(valInfo.val));
            cmds.doDelElems(keyInfos[i], valInfo);
        }

        return delCnt;
    }

    private boolean doDel(ByteBuf key) throws RedisResponseException {
        RedisKeyInfo keyInfo = makeRedisKeyInfo(key);
        
        RedisValueInfo valInfo = doDelGetVal(makeRedisKeyInfo(key));
        if (!valInfo.isValid()) {
            return false;
        }

        CommandsBase cmds = cmdHandlers.getCommandsByValueType(
            getValueType(valInfo.val));
        cmds.doDelElems(keyInfo, valInfo);
        return true;
    }

    private boolean doExists(RedisKeyInfo keyInfo)
        throws RedisResponseException {
        PreparedStatement pStmt = pstmtCache.getByRef(SQL_EXISTS_ONE);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
        pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));

        // returns true if there are any results, false otherwise
        return processQuery(pStmt, row -> true);
    }

    private int doExists(RedisKeyInfo[] keyInfos)
        throws RedisResponseException {
        ArrayValue keyIds = makeKeyIdsValue(keyInfos);
        PreparedStatement pStmt = pstmtCache.getByRef(SQL_EXISTS_ONE);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfos[0].slot));
        pStmt.setVariable(SQL_KEY_IDS, keyIds);

        final int [] res = { -1 };
        if (!processQuery(pStmt, row -> {
            if (res[0] != -1) {
                throw RedisResponseException.nosql(ERR_NO_SINGLE_RES);
            }
            res[0] = getIntField(row, FLD_RES);
            return true;
        })) {
            throw RedisResponseException.nosql("Missing query result");
        };

        return res[0];
    }

    // It is not clear how important it is for COPY to be atomic since we are
    // only updating one key. The problem is that it is not easy to condition
    // on the source version, since we are not updating the source key. For
    // existing destination, we could use SQL UPDATE statement, but not if the
    // destination doesn't exist yet. It seems the only way in this case would
    // be to perform unnecessary write on the source, which is not desirable.
    // This needs to be further discussed.
    private boolean doCopy(ByteBuf srcKeyBuf, ByteBuf dstKeyBuf,
        boolean toReplace, boolean throwIfNotFound)
        throws RedisResponseException {
        RedisKeyInfo srcKeyInfo = makeRedisKeyInfo(srcKeyBuf);
        RedisValueInfo srcVal = doGet(srcKeyInfo);
        if (!srcVal.isValid()) {
            if (!throwIfNotFound) {
                return false;
            }
            throw new RedisResponseException(ErrorPrefix.ERR, "no such key");
        }

        RedisKeyInfo dstKeyInfo = makeRedisKeyInfo(dstKeyBuf);

        CommandsBase cmds = cmdHandlers.getCommandsByValueType(
            getValueType(srcVal.val));
        MapValue newVal = cmds.doCopy(srcKeyInfo, srcVal, dstKeyInfo);
        return doSet(dstKeyInfo, newVal, srcVal.exp, !toReplace);
    }

    // Rename must be atomic, which means source and destination keys must
    // be in the same hash slot.
    private boolean doRename(ByteBuf srcKeyBuf, ByteBuf dstKeyBuf,
        boolean isNX) throws RedisResponseException {
        RedisKeyInfo srcKeyInfo = makeRedisKeyInfo(srcKeyBuf);
        RedisKeyInfo dstKeyInfo = makeRedisKeyInfo(dstKeyBuf);
        if (srcKeyInfo.slot != dstKeyInfo.slot) {
            throw RedisResponseException.crossSlot();
        }
        // renaming to the same name is a no-op
        if (srcKeyInfo.id.equals(dstKeyInfo.id)) {
            return true;
        }

        RedisValueInfo srcVal = doGet(srcKeyInfo);
        if (!srcVal.isValid()) {
            throw new RedisResponseException(ErrorPrefix.ERR, "no such key");
        }

        RedisValueInfo dstVal = RedisValueInfo.NONE;
        if (isNX) {
            dstVal = doGet(dstKeyInfo);
            if (dstVal.isValid()) {
                return false;
            }
        }

        CommandsBase cmds = cmdHandlers.getCommandsByValueType(
            getValueType(srcVal.val));
        MapValue newVal = cmds.doCopy(srcKeyInfo, srcVal, dstKeyInfo);

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        // RENAME transfers the source expiration time to the destination.
        PutRequest putReq = makePutReqForSet(dstKeyInfo, srcVal.exp,
            newVal, isNX ? dstVal : null);
        wmReq.add(putReq, true);

        DeleteRequest delReq = new DeleteRequest()
            .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
            .setKey(makePrimaryKey(srcKeyInfo)).setMatchVersion(srcVal.ver);
        wmReq.add(delReq, true);

        try {
            WriteMultipleResult wmRes = nosqlHandle.writeMultiple(wmReq);
            if (!wmRes.getSuccess()) {
                if (!isNX && wmRes.getFailedOperationIndex() == 0) {
                    throw RedisResponseException.nosql(
                        ERR_UNCONDITIONAL_PUT_FAIL);
                }
                throw new RedisRetryException();
            }
            return true;
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    public GenericCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache, CommandHandlers cmdHandlers) {
        super(nosqlHandle, cmdHandlers.getConfig(), pstmtCache);
        this.cmdHandlers = cmdHandlers;
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_COPY, this::handleCopy);
        cmdMap.put(CMD_DEL, this::handleDel);
        cmdMap.put(CMD_EXISTS, this::handleExists);
        cmdMap.put(CMD_PEXIPRETIME, this::handlePExpireTime);
        cmdMap.put(CMD_EXPIRETIME, this::handleExpireTime);
        cmdMap.put(CMD_PTTL, this::handlePTTL);
        cmdMap.put(CMD_TTL, this::handleTTL);
        cmdMap.put(CMD_PEXIPRE, this::handlePExpire);
        cmdMap.put(CMD_PEXIPREAT, this::handlePExpireAt);
        cmdMap.put(CMD_EXIPRE, this::handleExpire);
        cmdMap.put(CMD_EXIPREAT, this::handleExpireAt);
        cmdMap.put(CMD_PERSIST, this::handlePersist);
        cmdMap.put(CMD_TYPE, this::handleType);
        cmdMap.put(CMD_SCAN, this::handleScan);
        cmdMap.put(CMD_KEYS, this::handleKeys);
        cmdMap.put(CMD_RENAME, this::handleRename);
        cmdMap.put(CMD_RENAMENX, this::handleRenameNX);
    }

    public RedisMessage handleDel(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 1);

        // Delete is atomic with number of keys <= MAX_WM_CNT, otherwise
        // performed in batches.
        if (cmd.args.length == 1) {
            return doDel(cmd.args[0]) ? oneReply : zeroReply;
        } else {
            int delCnt = 0;
            int off = 0;
            do {
                int cnt = Math.min(MAX_WM_CNT, cmd.args.length - off);
                delCnt += doDel(cmd.args, off, cnt);
                off += MAX_WM_CNT;
            } while(off < cmd.args.length);
            return new IntegerRedisMessage(delCnt);
        }
    }

    public RedisMessage handleExists(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        int cnt = cmd.args.length == 1 ?
            (doExists(makeRedisKeyInfo(cmd.args[0])) ? 1 : 0) :
            doExists(makeRedisMultiKeyInfo(cmd.args, 0, cmd.args.length));
        return new IntegerRedisMessage(cnt);
    }

    public RedisMessage handlePExpireTime(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return doGetExp(cmd, (exp, curr) -> exp);
    }

    public RedisMessage handleExpireTime(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        return doGetExp(cmd, (exp, curr) -> millisToSeconds(exp));
    }

    public RedisMessage handlePTTL(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return doGetExp(cmd, (exp, curr) -> exp - curr);
    }

    public RedisMessage handleTTL(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        return doGetExp(cmd, (exp, curr) -> millisToSeconds(exp - curr));
    }

    public RedisMessage handlePExpire(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException
    {
        return doSetExp(cmd, TTLMode.PX);
    }

    public RedisMessage handlePExpireAt(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException
    {
        return doSetExp(cmd, TTLMode.PXAT);
    }

    public RedisMessage handleExpire(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException
    {
        return doSetExp(cmd, TTLMode.EX);
    }

    public RedisMessage handleExpireAt(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException
    {
        return doSetExp(cmd, TTLMode.EXAT);
    }

    public RedisMessage handlePersist(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException
    {
        chkExactNumArgs(cmd, 1);
        return doSetExp(cmd.args[0], NO_EXP, TTLOpt.XX);
    }

    public RedisMessage handleType(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        chkExactNumArgs(cmd, 1);
        RedisValueInfo valInfo = doGet(cmd.args[0]);
        if (!valInfo.isValid()) {
            return new SimpleStringRedisMessage("none");
        }
        return new SimpleStringRedisMessage(getValueType(valInfo.val));
    }

    public RedisMessage handleCopy(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        // source and destination
        chkMinNumArgs(cmd, 2);
        boolean toReplace = false;
        long dbNum = 0;
        for(int i = 2; i < cmd.args.length; i++) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("REPLACE")) {
                toReplace = true;
            } else if (arg.equalsIgnoreCase("DB") &&
                i < cmd.args.length - 1) {
                dbNum = Utils.byteBufToLong(cmd.args[++i]);
                chkDbIdx(dbNum);
            } else {
                throw RedisResponseException.syntaxError();
            }
        }
        
        return doCopy(cmd.args[0], cmd.args[1], toReplace, false) ?
            oneReply : zeroReply;
    }

    public RedisMessage handleRename(RedisClientContext client, 
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        boolean res = doWithRetries(
            () -> doRename(cmd.args[0], cmd.args[1], false));
        assert res;
        return okReply;
    }

    public RedisMessage handleRenameNX(RedisClientContext client, 
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        boolean res = doWithRetries(
            () -> doRename(cmd.args[0], cmd.args[1], true));
        return res ? oneReply : zeroReply;
    }

    public RedisMessage handleScan(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        chkMinNumArgs(cmd, 1);

        // SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
        if (cmd.args.length % 2 == 0) {
            throw RedisResponseException.syntaxError();
        }
        
        long cursor;
        try {
            cursor = Utils.byteBufToLong(cmd.args[0]);
        } catch (RedisResponseException ex) {
            throw new RedisResponseException(ErrorPrefix.ERR,
            "invalid cursor");
        }

        ByteBuf match = null;
        String type = null;
        long count = Scan.DEFAULT_COUNT;

        for(int i = 1; i < cmd.args.length; i += 2) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("MATCH")) {
                match = cmd.args[i + 1];
            } else if (arg.equalsIgnoreCase("COUNT")) {
                count = Utils.byteBufToLong(cmd.args[i + 1]);
                if (count <= 0) {
                    throw RedisResponseException.syntaxError();
                }
            } else if (arg.equalsIgnoreCase("TYPE")) {
                type = Utils.byteBufToString(cmd.args[i + 1]);
            } else {
                throw RedisResponseException.syntaxError();
            }
        }

        try(KeyScan scan = new KeyScan(this, cursor, match, count, type)) {
            return scan.scan();
        }
    }

    public RedisMessage handleKeys(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        chkExactNumArgs(cmd, 1);
        ArrayList<RedisMessage> results = new ArrayList<>();
        long cursor = 0;
        do {
            try(KeyScan scan = new KeyScan(this, cursor, cmd.args[0], 8092,
                null)) {
                cursor = scan.scan(results);
            }
        } while (cursor != 0);

        return new ArrayRedisMessage(results);
    }

}
