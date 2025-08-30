package oracle.nosql.redis.commands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisServerConfig;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

import static oracle.nosql.redis.util.Utils.*;

public class ServerManagementCommands extends CommandsBase {

    private static final String SQL_GET_NUM_KEYS =
        "SELECT count(*) AS res FROM redis";
    private static final String SQL_DEL_ALL_KEYS = "DELETE FROM redis";
    private static final String SQL_DEL_ALL_LIST_ELEMS =
        "DELETE FROM redis.lists";
    private static final String SQL_DEL_ALL_HASH_ELEMS =
        "DELETE FROM redis.hashes";

    private static final String INFO_SERVER = "Server";
    private static final String INFO_PERSISTENCE = "Persistence";
    private static final String[] INFO_PERSISTENCE_DATA = {
        "rdb_bgsave_in_progress:0"
    };
    private static final String[] INFO_SERVER_DATA = {
        "redis_version:7.2.0"
    };

    private static final String[] INFO_ALL_SECTS = {
        INFO_SERVER,
        INFO_PERSISTENCE
    };

    public static final String CMD_FLUSHDB = "FLUSHDB";
    public static final String CMD_FLUSHALL = "FLUSHALL";
    public static final String CMD_DBSIZE = "DBSIZE";
    public static final String CMD_CONFIG = "CONFIG";
    public static final String CMD_INFO = "INFO";

    public ServerManagementCommands(NoSQLHandle nosqlHandle,
        RedisServerConfig config, PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_FLUSHDB, this::handleFlushDB);
        cmdMap.put(CMD_FLUSHALL, this::handleFlushDB);
        cmdMap.put(CMD_DBSIZE, this::handleDBSize);
        cmdMap.put(CMD_CONFIG, this::handleConfig);
        cmdMap.put(CMD_INFO, this::handleInfo);
    }

    public RedisMessage handleFlushDB(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 0, 1);
        if (cmd.args != null && cmd.args.length == 1) {
            String arg = Utils.byteBufToString(cmd.args[0]);
            if (arg.equalsIgnoreCase("ASYNC")) {
                throw RedisResponseException.unsupported("FLUSHDB ASYNC");
            } else if (!arg.equalsIgnoreCase("SYNC")) {
                throw RedisResponseException.syntaxError();
            }
        }

        processQuery(pstmtCache.getByRef(SQL_DEL_ALL_KEYS));
        processQuery(pstmtCache.getByRef(SQL_DEL_ALL_LIST_ELEMS));
        processQuery(pstmtCache.getByRef(SQL_DEL_ALL_HASH_ELEMS));

        return okReply;
    }

    public RedisMessage handleDBSize(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 0);
        long cnt = getLongField(singleRowQuery(
            pstmtCache.getByRef(SQL_GET_NUM_KEYS)), FLD_RES);
        return new IntegerRedisMessage(cnt);
    }

    // For unit tests.
    public RedisMessage handleConfig(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        String sub = Utils.byteBufToString(cmd.args[0]);
        if (sub.equalsIgnoreCase("SET") || sub.equalsIgnoreCase("REWRITE") ||
            sub.equalsIgnoreCase("RESETSTAT")) {
            return okReply;
        } else if (sub.equalsIgnoreCase("GET")) {
            return ArrayRedisMessage.EMPTY_INSTANCE;
        } else {
            throw RedisResponseException.syntaxError();
        }
    }

    // For testing. Redis-cli sends this command after connecting.
    public RedisMessage handleInfo(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        String[] sects = INFO_ALL_SECTS;
        if (cmd.args != null && cmd.args.length != 0) {
            sects = Arrays.stream(cmd.args).map(arg ->
                lambdaUnchecked(() -> Utils.byteBufToString(arg)))
                .distinct().toArray(String[]::new);
        }

        // Currently the following is used to run existing Redis tests.
        StringBuilder sb = null;
        for(String sect: sects) {
            String[] data = null;
            switch (sect) {
                case INFO_SERVER:
                    data = INFO_SERVER_DATA;
                    break;
                case INFO_PERSISTENCE:
                    data = INFO_PERSISTENCE_DATA;
                    break;
                default:
                    break;
            }

            if (data == null) {
                continue;
            }

            if (sb == null) {
                sb = new StringBuilder();
            } else {
                // Sections are separated by blank lines.
                sb.append("\r\n");
            }

            for(String prop: data) {
                sb.append(prop).append("\r\n");
            }
        }

        return sb != null ?
            new FullBulkStringRedisMessage(stringToByteBuf(sb.toString())) :
            FullBulkStringRedisMessage.EMPTY_INSTANCE;
    }

}
