/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
import oracle.nosql.redis.util.PreparedStatementCache;

public class ConnectionCommands extends CommandsBase {

    public static final String CMD_PING = "PING";
    public static final String CMD_ECHO = "ECHO";
    public static final String CMD_QUIT = "QUIT";
    public static final String CMD_INFO = "INFO";
	 
    public ConnectionCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_PING, this::handlePing);
        cmdMap.put(CMD_ECHO, this::handleEcho);
        cmdMap.put(CMD_QUIT, this::handleQuit);
        cmdMap.put(CMD_INFO, this::handleInfo);
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

    // INFO is actually a server management command.  For now we just stub it
    // here for redis-cli to work (for testing).  Redis-cli sends this command
    // after connecting.
    public RedisMessage handleInfo(RedisClientContext client,
        RawCommand cmd) {
        return okReply;
    }

}
