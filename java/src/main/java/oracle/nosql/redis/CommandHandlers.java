/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import java.util.HashMap;

import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.commands.CommandsBase;
import oracle.nosql.redis.commands.ConnectionCommands;
import oracle.nosql.redis.commands.GenericCommands;
import oracle.nosql.redis.commands.HashCommands;
import oracle.nosql.redis.commands.JSONCommands;
import oracle.nosql.redis.commands.ListCommands;
import oracle.nosql.redis.commands.StringCommands;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;

public class CommandHandlers {
	
    @FunctionalInterface
    public interface CommandHandler extends
        ThrowingBiFunction<RedisClientContext,RawCommand,RedisMessage,
        RedisResponseException>{
    }

    private final HashMap<String, CommandHandler> cmdMap = new HashMap<>();
    private final NoSQLHandle nosqlHandle;
    private final PreparedStatementCache pstmtCache;

    private ConnectionCommands connCommands;
    private GenericCommands genericCommands;
    private StringCommands stringCommands;
    private ListCommands listCommands;
    private HashCommands hashCommands;
    private JSONCommands jsonCommands;
    
    CommandHandlers(NoSQLHandle nosqlHandle) {
        this.nosqlHandle = nosqlHandle;
        pstmtCache = new PreparedStatementCache(nosqlHandle);        
    }

    void init() {
        connCommands = new ConnectionCommands(nosqlHandle, pstmtCache);
        genericCommands = new GenericCommands(nosqlHandle, pstmtCache, this);
        stringCommands = new StringCommands(nosqlHandle, pstmtCache);
        listCommands = new ListCommands(nosqlHandle, pstmtCache);
        hashCommands = new HashCommands(nosqlHandle, pstmtCache);
        jsonCommands = new JSONCommands(nosqlHandle, pstmtCache);

        connCommands.registerCommands(cmdMap);
        genericCommands.registerCommands(cmdMap);
        stringCommands.registerCommands(cmdMap);
        listCommands.registerCommands(cmdMap);
        hashCommands.registerCommands(cmdMap);
        jsonCommands.registerCommands(cmdMap);
    }

    CommandHandler getHandler(String name) {
        return cmdMap.get(name);
    }

    public CommandsBase getCommandsByValueType(String type)
        throws RedisResponseException {
        switch(type) {
            case CommandsBase.TYPE_STRING:
                return stringCommands;
            case CommandsBase.TYPE_LIST:
                return listCommands;
            case CommandsBase.TYPE_HASH:
                return hashCommands;
            default:
                throw RedisResponseException.corrupt(
                    "Value of unknown type " + type);
        }
    }

}
