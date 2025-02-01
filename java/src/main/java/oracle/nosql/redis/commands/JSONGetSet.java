package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.RawCommand;
import oracle.nosql.redis.RedisClientContext;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

public class JSONGetSet extends JSONCommandsBase {

    public JSONGetSet(NoSQLHandle nosqlHandle,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private boolean doJSONSet(RedisKeyInfo keyInfo, String path,
        FieldValue val, SetOpt setOpt) throws RedisResponseException {
        TranslateSetResult tr = translateSetPath(path);
        String sql = tr.getSQLDecl() + DECL_KEY_ID_VAL +
            (tr.sqlPutPath == null || setOpt == SetOpt.XX ?
                String.format(SQL_UPDATE_SET_FMT, tr.sqlPath) :
                String.format(SQL_UPDATE_PUT_FMT, tr.sqlPutPath,
                    setOpt == SetOpt.NX ? tr.getSQLNXFilter() : "",
                    tr.getSQLNewFieldsExpr()));
        
        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        pStmt.setVariable(SQL_VAL, transformValue(val));
        return getNumRowsUpdated(doSQLUpdate(pStmt, false)) == 1;
    }

    private FieldValue doJSONGet(RedisKeyInfo keyInfo, List<String> paths)
        throws RedisResponseException {
        assert !paths.isEmpty();
        TranslateResult tr = paths.size() == 1 ?
            translatePath(paths.get(0)) : translatePaths(paths);
        String sql = tr.getSQLDecl() + DECL_KEY_ID +
            String.format(SQL_GET_FMT, tr.sqlPath);

        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        
        MapValue row = doSQLGet(pStmt);
        if (row == null) {
            return null;
        }

        ArrayValue res = getArrRes(doSQLGet(pStmt));
        return paths.size() == 1 ?
            untransformValues(res) : makeMultiResult(paths, res);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_SET, this::handleJSONSet);
        cmdMap.put(CMD_JSON_GET, this::handleJSONGet);
    }

    public RedisMessage handleJSONSet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 3, 4);
        
        String path = Utils.byteBufToString(cmd.args[1]);
        ByteBuf val = cmd.args[2];
        SetOpt setOpt = null;

        if (cmd.args.length == 4) {
            String arg = Utils.byteBufToString(cmd.args[3]);
            if (arg.equalsIgnoreCase("NX")) {
                chkNotSet(setOpt);
                setOpt = SetOpt.NX;
            } else if (arg.equalsIgnoreCase("XX")) {
                chkNotSet(setOpt);
                setOpt = SetOpt.XX;
            } else {
                throw RedisResponseException.syntaxError();
            }
        }

        // With root path, we may either set new value or overwrite existing
        // value, so we use doGetSet() approach.
        if (path.equals(ROOT_PATH)) {
            final SetOpt setOpt1 = setOpt; // copy to use in doGetSet()
            return doGetSet(
                cmd.args[0],
                (oldVal) -> setOpt1 == null ||
                    (setOpt1 == SetOpt.NX && !oldVal.exists()) ||
                    (setOpt1 == SetOpt.XX && oldVal.exists()),
                (oldVal) -> {
                    if (oldVal.val != null &&
                        !getValueType(oldVal.val).equals(TYPE_JSON)) {
                        throw RedisResponseException.wrongType();
                    }
                    return new RedisValueInfo(makeJSONValue(val), null,
                        oldVal.exp);
                },
                // newVal is NONE if SET is not successful
                (oldVal, newVal) -> {
                    assert(setOpt1 != null || newVal.exists());
                    return newVal.exists() ?
                        okReply : FullBulkStringRedisMessage.NULL_INSTANCE;
                }, true);
        }

        // With any other path, we can only update existing value. To do this
        // in 1 request (vs doing GET and PUT), we use JSONPathToSQL
        // translator to construct an update statement.
        return doJSONSet(makeRedisKeyInfo(cmd.args[0]), path,
            byteBufToJson(val), setOpt) ?
            okReply : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleJSONGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        
        ArrayList<String> paths = new ArrayList<>();
        String indent = null;
        String newLine = null;
        String space = null;

        for(int i = 1; i < cmd.args.length; i++) {
            String arg = Utils.byteBufToString(cmd.args[i]);
            if (arg.equalsIgnoreCase("INDENT")) {
                chkMinNumArgs(cmd, i + 2);
                indent = Utils.byteBufToString(cmd.args[++i]);
            } else if (arg.equalsIgnoreCase("NEWLINE")) {
                chkMinNumArgs(cmd, i + 2);
                newLine = Utils.byteBufToString(cmd.args[++i]);
            } if (arg.equalsIgnoreCase("SPACE")) {
                chkMinNumArgs(cmd, i + 2);
                space = Utils.byteBufToString(cmd.args[++i]);
            } else {
                paths.add(arg);
            }
        }

        if (paths.isEmpty()) {
            paths.add(ROOT_PATH);
        }

        FieldValue res = doJSONGet(makeRedisKeyInfo(cmd.args[0]), paths);
        if (res == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        // TODO: fully support formatting options above.
        JsonOptions jsonOpts =
            indent != null || newLine != null || space != null ?
            JsonOptions.PRETTY : null;
        return new FullBulkStringRedisMessage(
            Utils.stringToByteBuf(res.toJson(jsonOpts)));
    }

}
