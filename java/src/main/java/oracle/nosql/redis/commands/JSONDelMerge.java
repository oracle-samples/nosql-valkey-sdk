package oracle.nosql.redis.commands;

import java.util.HashMap;

import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONDelMerge extends JSONCommandsBase {

    private static final String SQL_DEL_KEY =
        SQL_DECLARE + DECL_KEY_ID + "DELETE FROM redis $r WHERE " +
        KEY_ID_COND + IS_TYPE_JSON + SQL_RETURNING + NOT_EXPIRED + "AS res";

    // Note that since we store array elements wrapped in objects, removing
    // array element(s) using the sqlPath will only remove actual values, not
    // the wrapper objects. We use the 2nd REMOVE clause applied on the parent
    // path and only if the parent path points to an array, to remove empty
    // object wrappers from the array.
    private static final String SQL_UPDATE_DEL_FMT = DECL_KEY_ID +
        "UPDATE redis $r SET $r.value.pad = size([%s]), REMOVE %s, " +
        "REMOVE %s[size($element) = 0] WHERE " + SQL_JSON_COND +
        SQL_RETURNING_PAD;

    private static final String NON_ZERO_SIZE = " AND size($value) != 0";
    private static final String NON_EMPTY_MAP_FILTER = MAP_FILTER +
        NON_ZERO_SIZE;
    private static final String NON_EMPTY_ARR_FILTER = ARR_FILTER +
        NON_ZERO_SIZE;
    private static final String NON_ZERO_NUM_FILTER = NUM_FILTER +
        " AND $value != 0";

    private static final String SQL_CLEAR_FMT = DECL_KEY_ID +
        "UPDATE redis $r SET $r.value.pad = size([%s]) + size([%s]) + " +
        "size([%s]), SET %s = {}, SET %s = [], SET %s = 0 WHERE" +
        SQL_JSON_COND + SQL_RETURNING_PAD;

    

    public JSONDelMerge(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_DEL, this::handleJSONDel);
        cmdMap.put(CMD_JSON_FORGET, this::handleJSONDel);
        cmdMap.put(CMD_JSON_CLEAR, this::handleJSONClear);
        cmdMap.put(CMD_JSON_MERGE, this::handleJSONMerge);
    }

    public RedisMessage handleJSONDel(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);

        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;
        
        if (path.equals(ROOT_PATH)) {
            PreparedStatement pStmt = pstmtCache.getByRef(SQL_DEL_KEY);
            pStmt.setVariable("$keyId", new StringValue(keyInfo.id));
            
            MapValue row = doSQLUpdate(pStmt);
            if (row == null) {
                return zeroReply;
            }

            return getBoolRes(row) ? oneReply : zeroReply;
        }

        TranslateResultWithFilters tr = translatePathWithFilter(path, null,
            ARR_FILTER);
        String sql = tr.getSQLDecl() + String.format(SQL_UPDATE_DEL_FMT,
            tr.sqlPath, tr.sqlPath, tr.parentSQLPathWithFilter());
        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            return zeroReply;
        }

        return new IntegerRedisMessage(getIntRes(row));
    }

    public RedisMessage handleJSONClear(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        String path = cmd.args.length > 1 ?
            Utils.byteBufToString(cmd.args[1]) : ROOT_PATH;

        TranslateResultWithFilters tr = translatePathWithFilters(path,
            new String[] { NON_EMPTY_MAP_FILTER, NON_EMPTY_ARR_FILTER,
                NON_ZERO_NUM_FILTER }, null);
        String sql = tr.getSQLDecl() + String.format(SQL_CLEAR_FMT,
            tr.sqlPathsWithFilter[0], tr.sqlPathsWithFilter[1],
            tr.sqlPathsWithFilter[2], tr.sqlPathsWithFilter[0],
            tr.sqlPathsWithFilter[1], tr.sqlPathsWithFilter[2]);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }

        return new IntegerRedisMessage(getIntRes(row));        
    }

    public RedisMessage handleJSONMerge(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        return null;
    }

}
