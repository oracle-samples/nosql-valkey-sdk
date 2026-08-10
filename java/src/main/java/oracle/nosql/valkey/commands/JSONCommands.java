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

public class JSONCommands extends JSONCommandsBase {

    private final JSONGetSet jsonGetSet;
    private final JSONValueOps jsonValueOps;
    private final JSONArrays jsonArrays;
    private final JSONDel jsonDel;

    public JSONCommands(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
        jsonGetSet = new JSONGetSet(nosqlHandle, config, pstmtCache);
        jsonValueOps = new JSONValueOps(nosqlHandle, config, pstmtCache);
        jsonArrays = new JSONArrays(nosqlHandle, config, pstmtCache);
        jsonDel = new JSONDel(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        jsonGetSet.registerCommands(cmdMap);
        jsonValueOps.registerCommands(cmdMap);
        jsonArrays.registerCommands(cmdMap);
        jsonDel.registerCommands(cmdMap);
    }

}
