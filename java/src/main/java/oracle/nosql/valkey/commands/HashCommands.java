/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.HashMap;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.valkey.CommandHandlers.CommandHandler;
import oracle.nosql.valkey.RedisServerConfig;
import oracle.nosql.valkey.util.PreparedStatementCache;

public class HashCommands extends HashCommandsBase {

    private final HashUpdate hashUpdate;
    private final HashRead hashRead;

    public HashCommands(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
        hashUpdate = new HashUpdate(nosqlHandle, config, pstmtCache);
        hashRead = new HashRead(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        hashUpdate.registerCommands(cmdMap);
        hashRead.registerCommands(cmdMap);
    }

}
