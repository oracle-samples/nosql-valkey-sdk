/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.util.HashMap;

import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class ConnectionCommands extends CommandsBase {

    public static final String CMD_PING = "PING";
    public static final String CMD_ECHO = "ECHO";
    public static final String CMD_QUIT = "QUIT";
    public static final String CMD_HELLO = "HELLO";
    public static final String CMD_CLIENT = "CLIENT";

    public ConnectionCommands(NoSQLHandle nosqlHandle,
        RedisServerConfig config, PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_PING, this::handlePing);
        cmdMap.put(CMD_ECHO, this::handleEcho);
        cmdMap.put(CMD_QUIT, this::handleQuit);
        cmdMap.put(CMD_HELLO, this::handleHello);
        cmdMap.put(CMD_CLIENT, this::handleClient);
    }

    public RedisMessage handlePing(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        if (cmd.args == null) {
            return new SimpleStringRedisMessage("PONG");
        }
        chkExactNumArgs(cmd, 1);
        return new FullBulkStringRedisMessage(cmd.args[0].retain());
    }

    public RedisMessage handleEcho(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 1);
        return new FullBulkStringRedisMessage(cmd.args[0].retain());            
    }

    public RedisMessage handleQuit(RedisClientContext client,
        RawCommand cmd) {
        return okReply;
    }

    // Stub for unit tests.
    public RedisMessage handleHello(RedisClientContext client,
        RawCommand cmd) {
        return okReply;
    }

    public RedisMessage handleClient(RedisClientContext client, RawCommand cmd)
        throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        String sub = Utils.byteBufToString(cmd.args[0]);
        if (sub.equalsIgnoreCase("SETINFO") ||
            sub.equalsIgnoreCase("SETNAME")) {
            return okReply;
        }

        throw RedisResponseException.unsupported(cmd.name + " " + sub);
    }

}
