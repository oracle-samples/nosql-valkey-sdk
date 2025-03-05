package oracle.nosql.redis.commands;

import java.util.HashMap;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.util.PreparedStatementCache;

public class JSONCommands extends JSONCommandsBase {

    private final JSONGetSet jsonGetSet;
    private final JSONValueOps jsonValueOps;
    private final JSONArrays jsonArrays;
    private final JSONDel jsonDel;

    public JSONCommands(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
        jsonGetSet = new JSONGetSet(nosqlHandle, pstmtCache);
        jsonValueOps = new JSONValueOps(nosqlHandle, pstmtCache);
        jsonArrays = new JSONArrays(nosqlHandle, pstmtCache);
        jsonDel = new JSONDel(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        jsonGetSet.registerCommands(cmdMap);
        jsonValueOps.registerCommands(cmdMap);
        jsonArrays.registerCommands(cmdMap);
        jsonDel.registerCommands(cmdMap);
    }

}
