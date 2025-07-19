package oracle.nosql.redis.commands;

import java.util.HashMap;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;

public class ListCommands extends ListCommandsBase {

    private final ListPushPop listPushPop;
    private final ListRead listRead;
    private final ListSetInsert listSetInsert;
    private final ListTrimRemove listTrimRemove;

    public ListCommands(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        // Technically we don't want to store nosqlHandle, pstmtCache in this
        // class, but its possible some implementations will be added here
        // that will use these fields.
        super(nosqlHandle, config, pstmtCache);
        listPushPop = new ListPushPop(nosqlHandle, config, pstmtCache);
        listRead = new ListRead(nosqlHandle, config, pstmtCache);
        listSetInsert = new ListSetInsert(nosqlHandle, config, pstmtCache);
        listTrimRemove = new ListTrimRemove(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        listPushPop.registerCommands(cmdMap);
        listRead.registerCommands(cmdMap);
        listSetInsert.registerCommands(cmdMap);
        listTrimRemove.registerCommands(cmdMap);
    }

}
