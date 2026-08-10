/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.valkey.commands.*;
import oracle.nosql.valkey.util.PreparedStatementCache;
import oracle.nosql.valkey.util.Utils.ThrowingBiFunction;

public class CommandHandlers {
	
    @FunctionalInterface
    public interface CommandHandler extends
        ThrowingBiFunction<RedisClientContext,RawCommand,RedisMessage,
        RedisResponseException>{
    }

    private final HashMap<String, CommandHandler> cmdMap = new HashMap<>();
    private final NoSQLHandle nosqlHandle;
    private final RedisServerConfig config;
    private final ChannelGroup clientChannels;
    private final PreparedStatementCache pstmtCache;

    private ConnectionCommands connCommands;
    private GenericCommands genericCommands;
    private ServerManagementCommands serverManagementCommands;
    private StringCommands stringCommands;
    private ListCommands listCommands;
    private HashCommands hashCommands;
    private JSONCommands jsonCommands;
    
    CommandHandlers(NoSQLHandle nosqlHandle, RedisServerConfig config,
        ChannelGroup clientChannels) {
        this.nosqlHandle = nosqlHandle;
        this.config = config;
        this.clientChannels = clientChannels;
        pstmtCache = new PreparedStatementCache(nosqlHandle);        
    }

    void init() {
        connCommands = new ConnectionCommands(nosqlHandle, config, pstmtCache);
        genericCommands = new GenericCommands(nosqlHandle, pstmtCache, this);
        serverManagementCommands = new ServerManagementCommands(nosqlHandle,
            config, pstmtCache, clientChannels);
        stringCommands = new StringCommands(nosqlHandle, config, pstmtCache);
        listCommands = new ListCommands(nosqlHandle, config, pstmtCache);
        hashCommands = new HashCommands(nosqlHandle, config, pstmtCache);
        jsonCommands = new JSONCommands(nosqlHandle, config, pstmtCache);

        connCommands.registerCommands(cmdMap);
        genericCommands.registerCommands(cmdMap);
        serverManagementCommands.registerCommands(cmdMap);
        stringCommands.registerCommands(cmdMap);
        listCommands.registerCommands(cmdMap);
        hashCommands.registerCommands(cmdMap);
        jsonCommands.registerCommands(cmdMap);
    }

    CommandHandler getHandler(String name) {
        return cmdMap.get(name);
    }

    void scheduleElemTablesCleanup(ExecutorService execSvc) {
        execSvc.execute(() -> { listCommands.cleanupElemsTable(); });
        execSvc.execute(() -> { hashCommands.cleanupElemsTable(); });
    }

    public RedisServerConfig getConfig() { return config; }

    public CommandsBase getCommandsByValueType(String type)
        throws RedisResponseException {
        switch(type) {
            case CommandsBase.TYPE_STRING:
                return stringCommands;
            case CommandsBase.TYPE_LIST:
                return listCommands;
            case CommandsBase.TYPE_HASH:
                return hashCommands;
            case CommandsBase.TYPE_JSON:
                return jsonCommands;
            default:
                throw RedisResponseException.corrupt(
                    "Value of unknown type " + type);
        }
    }

}
