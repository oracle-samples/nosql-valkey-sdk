package oracle.nosql.redis.commands;

import java.util.HashMap;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.util.PreparedStatementCache;

public class HashCommands extends HashCommandsBase {

    private final HashUpdate hashUpdate;
    private final HashRead hashRead;

    public HashCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
        hashUpdate = new HashUpdate(nosqlHandle, pstmtCache);
        hashRead = new HashRead(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        hashUpdate.registerCommands(cmdMap);
        hashRead.registerCommands(cmdMap);
    }

}
