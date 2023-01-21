/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import static oracle.nosql.redis.util.Utils.millisToSeconds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.LongBinaryOperator;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class GenericCommands extends CommandsBase {

    static enum TTLOpt {
        NX,
        XX,
        GT,
        LT
    }

    private static final long SCAN_DEF_COUNT = 10;

    private final Scan scan;

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
        long currTime = System.currentTimeMillis();
        return new IntegerRedisMessage(valInfo.isValid(currTime) ?
            (valInfo.exp == NO_EXP ? -1 :
                conv.applyAsLong(valInfo.exp, currTime)) : -2);
    }

    // For commands that set expiration time, such as
    // EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST.
    private RedisMessage doSetExp(ByteBuf keyBuf, long exp, TTLOpt ttlOpt)
        throws RedisResponseException {
        return doGetSet(keyBuf,
            (oldVal) -> oldVal.exists() &&
                (ttlOpt == null ||
                 (ttlOpt == TTLOpt.NX && oldVal.exp == NO_EXP) ||
                 (ttlOpt == TTLOpt.XX && oldVal.exp != NO_EXP) ||
                 (ttlOpt == TTLOpt.GT && compExp(exp, oldVal.exp) > 0) ||
                 (ttlOpt == TTLOpt.LT && compExp(exp, oldVal.exp) < 0)),
            (oldVal) -> new RedisValueInfo(oldVal.val, null, exp),
            (oldVal, newVal) -> newVal.exists() ? oneReply : zeroReply,
            true);
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

    private boolean doDel(ByteBuf keyBuf) throws RedisResponseException {
        return doGetDel(keyBuf).exists();
    }
 
    // Note that copy cannot be made atomic since src and dst can be on
    // different shards.
    private boolean doCopy(ByteBuf srcKeyBuf, ByteBuf dstKeyBuf,
        boolean toReplace, boolean throwIfNotFound)
        throws RedisResponseException {
        RedisValueInfo srcVal = doGet(srcKeyBuf);
        if (!srcVal.isValid()) {
            if (!throwIfNotFound) {
                return false;
            }
            throw new RedisResponseException(ErrorPrefix.ERR, "no such key");
        }

        return doGetSet(dstKeyBuf, (oldVal) -> toReplace || !oldVal.exists(),
            (oldVal) -> new RedisValueInfo(srcVal.val, null, srcVal.exp),
            (oldVal, newVal) -> newVal.exists(), !toReplace);
    }

    public GenericCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
        scan = new Scan(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put("COPY", this::handleCopy);
        cmdMap.put("DEL", this::handleDel);
        cmdMap.put("EXISTS", this::handleExists);
        cmdMap.put("PEXPIRETIME", this::handlePExpireTime);
        cmdMap.put("EXPIRETIME", this::handleExpireTime);
        cmdMap.put("PTTL", this::handlePTTL);
        cmdMap.put("TTL", this::handleTTL);
        cmdMap.put("PEXPIRE", this::handlePExpire);
        cmdMap.put("PEXPIREAT", this::handlePExpireAt);
        cmdMap.put("EXPIRE", this::handleExpire);
        cmdMap.put("EXPIREAT", this::handleExpireAt);
        cmdMap.put("PERSIST", this::handlePersist);
        cmdMap.put("TYPE", this::handleType);
        cmdMap.put("SCAN", this::handleScan);
        cmdMap.put("KEYS", this::handleKeys);
    }

    public RedisMessage handleDel(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        int cnt = 0;
        for(int i = 0; i < cmd.args.length; i++) {
            if (doDel(cmd.args[i])) {
                cnt++;
            }
        }
        return new IntegerRedisMessage(cnt);
    }

    public RedisMessage handleExists(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        int cnt = 0;
        long currTime = System.currentTimeMillis();
        for(int i = 0; i < cmd.args.length; i++) {
            RedisValueInfo valInfo = doGet(cmd.args[i]);
            if (valInfo.isValid(currTime)) {
                cnt++;
            }
        }
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

    // Note that it is impossible for us to make rename atomic, since it needs
    // to delete existing key, because the source and destination could be on
    // different shards.
    public RedisMessage handleRename(RedisClientContext client, 
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 2);
        boolean success = doCopy(cmd.args[0], cmd.args[1], true, true);
        assert success;
        success = doDel(cmd.args[0]);
        assert success;
        return okReply;
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
        long count = SCAN_DEF_COUNT;

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

        return scan.scan(cursor, match, count, type);
    }

    public RedisMessage handleKeys(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException
    {
        chkExactNumArgs(cmd, 1);
        ArrayList<RedisMessage> results = new ArrayList<>();
        long cursor = 0;
        do {
            cursor = scan.scan(cursor, cmd.args[0], 10, null, results);
        } while (cursor != 0);

        return new ArrayRedisMessage(results);
    }

}
