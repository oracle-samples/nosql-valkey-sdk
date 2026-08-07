/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.commands;

import java.util.HashMap;

import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONDel extends JSONCommandsBase {

    private static final String SQL_DEL_KEY =
        DECL_KEY_ID + "DELETE FROM valkey $r WHERE " + R_PK_COND +
        AND_IS_JSON + SQL_RETURNING + NOT_EXPIRED + "AS res";

    // Note that since we store array elements wrapped in objects, removing
    // array element(s) using the sqlPath will only remove actual values, not
    // the wrapper objects. We use the 2nd REMOVE clause applied on the parent
    // path and only if the parent path points to an array, to remove empty
    // object wrappers from the array.
    private static final String SQL_UPDATE_DEL_FMT = DECL_KEY_ID +
        "%sUPDATE valkey $r SET $r.value.pad = size([%s]), REMOVE %s, " +
        "REMOVE %s[size($element) = 0] WHERE " + SQL_EXISTS_COND +
        SQL_RETURNING_PAD + SQL_IS_JSON;

    private static final String NON_ZERO_SIZE = " AND size($value) != 0";
    private static final String NON_EMPTY_MAP_FILTER = MAP_FILTER +
        NON_ZERO_SIZE;
    private static final String NON_EMPTY_ARR_FILTER = ARR_FILTER +
        NON_ZERO_SIZE;
    private static final String NON_ZERO_NUM_FILTER = NUM_FILTER +
        " AND $value != 0";

    private static final String SQL_CLEAR_FMT = DECL_KEY_ID +
        "%sUPDATE valkey $r SET $r.value.pad = size([%s]) + size([%s]) + " +
        "size([%s]), SET %s = {}, SET %s = [], SET %s = 0 WHERE " +
        SQL_EXISTS_COND + SQL_RETURNING_PAD + SQL_IS_JSON;
    
    public JSONDel(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_DEL, this::handleJSONDel);
        cmdMap.put(CMD_JSON_FORGET, this::handleJSONDel);
        cmdMap.put(CMD_JSON_CLEAR, this::handleJSONClear);
    }

    public RedisMessage handleJSONDel(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);

        RedisKeyInfo keyInfo = makeRedisKeyInfo(cmd.args[0]);
        PathInfo pi = cmd.args.length > 1 ?
            PathInfo.get(cmd.args[1]) : PathInfo.ROOT;
        
        if (pi.isRoot) {
            PreparedStatement pStmt = pstmtCache.getByRef(SQL_DEL_KEY);
            pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfo.slot));
            pStmt.setVariable(SQL_KEY_ID, new StringValue(keyInfo.id));
            
            MapValue row = doSQLUpdate(pStmt);
            if (row == null) {
                return zeroReply;
            }

            return getBoolRes(row) ? oneReply : zeroReply;
        }

        TranslateResultWithFilters tr = translatePathWithFilter(pi, null,
            ARR_FILTER);
        String sql = String.format(SQL_UPDATE_DEL_FMT, tr.getSQLDecl(),
            tr.sqlPath, tr.sqlPath, tr.parentSQLPathWithFilter());
        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            return zeroReply;
        }

        chkIsJSON(row);
        return new IntegerRedisMessage(getIntRes(row));
    }

    public RedisMessage handleJSONClear(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 1, 2);
        PathInfo pi = cmd.args.length > 1 ?
            PathInfo.get(cmd.args[1]) : PathInfo.ROOT;

        TranslateResultWithFilters tr = translatePathWithFilters(pi,
            new String[] { NON_EMPTY_MAP_FILTER, NON_EMPTY_ARR_FILTER,
                NON_ZERO_NUM_FILTER }, null);
        String sql = String.format(SQL_CLEAR_FMT, tr.getSQLDecl(),
            tr.sqlPathsWithFilter[0], tr.sqlPathsWithFilter[1],
            tr.sqlPathsWithFilter[2], tr.sqlPathsWithFilter[0],
            tr.sqlPathsWithFilter[1], tr.sqlPathsWithFilter[2]);
        PreparedStatement pStmt = getPrepStmt(makeRedisKeyInfo(cmd.args[0]),
            sql, tr);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            throw new RedisResponseException(ERR_KEY_NOT_EXISTS);
        }

        chkIsJSON(row);
        return new IntegerRedisMessage(getIntRes(row));
    }

}
