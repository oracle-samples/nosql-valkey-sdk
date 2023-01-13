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
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils.ThrowingBiFunction;
import oracle.nosql.redis.commands.ConnectionCommands;
import oracle.nosql.redis.commands.GenericCommands;
import oracle.nosql.redis.commands.StringCommands;

public class CommandHandlers {
	
    @FunctionalInterface
    public interface CommandHandler extends
        ThrowingBiFunction<RedisClientContext,RawCommand,RedisMessage,
        RedisResponseException>{
    }

    private final HashMap<String, CommandHandler> cmdMap;
    private final PreparedStatementCache pstmtCache;
    private final ConnectionCommands connCommands;
    private final GenericCommands genericCommands;
    
    private final StringCommands stringCommands;
    
    CommandHandlers(NoSQLHandle nosqlHandle) {
        cmdMap = new HashMap<>();
        pstmtCache = new PreparedStatementCache(nosqlHandle);
        
        connCommands = new ConnectionCommands(nosqlHandle, pstmtCache);
        genericCommands = new GenericCommands(nosqlHandle, pstmtCache);
        stringCommands = new StringCommands(nosqlHandle, pstmtCache);

        connCommands.registerCommands(cmdMap);
        genericCommands.registerCommands(cmdMap);
        stringCommands.registerCommands(cmdMap);
    }

    CommandHandler getHandler(String name) {
        return cmdMap.get(name);
    }

}
