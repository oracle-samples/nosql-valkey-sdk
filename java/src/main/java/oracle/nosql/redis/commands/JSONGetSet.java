package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.*;
import oracle.nosql.redis.CommandHandlers.CommandHandler;
import oracle.nosql.redis.commands.jsonpath.JSONPathValueVisitor;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.RedisRetryException;
import org.antlr.v4.runtime.tree.ParseTree;

public class JSONGetSet extends JSONCommandsBase {

    private static final String SQL_UPDATE_SET_FMT =
        "UPDATE redis $r SET $r.value.pad = (EXISTS %s), %s = $val WHERE " +
        SQL_EXISTS_COND + SQL_RETURNING_PAD + SQL_IS_JSON;
    private static final String SQL_UPDATE_PUT_FMT =
        "UPDATE redis $r PUT %s %s WHERE " + SQL_EXISTS_COND + SQL_RETURNING +
        "EXISTS %s AS res" + SQL_IS_JSON;
    private static final String SQL_UPDATE_PUT_NX_FMT =
        "UPDATE redis $r SET $r.value.pad = (NOT EXISTS %s), " +
        "PUT %s[NOT EXISTS %s] %s WHERE " + SQL_EXISTS_COND + SQL_RETURNING +
        "$r.value.pad AND EXISTS %s AS res" + SQL_IS_JSON;
    private static final String SQL_GET_FMT = DECL_KEY_ID +
        "%sSELECT [%s] AS res " + SQL_IS_JSON + " FROM redis $r WHERE " +
        SQL_EXISTS_COND;
    private static final String SQL_MGET_FMT = DECL_KEY_IDS +
        "%sSELECT id, [%s] AS res FROM redis $r " + WHERE_KEY_IDS_COND +
        AND_NOT_EXPIRED + AND_IS_JSON;

    private static final String[] MAP_FILTER_ARR = new String[] { MAP_FILTER };
    private static final String SQL_MERGE_FMT = DECL_KEY_ID_VAL +
        "%sUPDATE redis $r JSON MERGE %s WITH PATCH %s WHERE " +
        SQL_EXISTS_COND + SQL_RETURNING +  "(EXISTS %s) AS res" +
        SQL_IS_JSON;
    private static final String SQL_MSET_GET = DECL_KEY_IDS +
        "SELECT $r.id, row_version($r) AS ver, $r.key.exp AS exp, " +
        "$r.value.json AS json FROM redis $r " + WHERE_KEY_IDS_COND;

    public JSONGetSet(NoSQLHandle nosqlHandle, RedisServerConfig config,
        PreparedStatementCache pstmtCache) {
        super(nosqlHandle, config, pstmtCache);
    }

    // Behavior when path has multiple items:
    // OK reply returned if update partially succeeded, that is only some of
    // the items were set. When using NX option, currently update will fail if
    // any items in the path exists. Redis JSON only allows static paths for
    // non-existing fields, so we can't compare it here. It is possible that
    // the better option would be to set only items in the path that don't
    // exist, however with current implementation it is not possible when
    // there are multiple leaf fields. We could change implementation to use
    // separate PUT clause and separate NX filter for each leaf field which
    // would enable each of these fields to be checked and set separately.
    // This may be considered. For XX, the behavior is the same as in UPDATE
    // SET clause, that is only existing fields are set and non-existing
    // skipped.
    private boolean doJSONSet(RedisKeyInfo keyInfo, PathInfo pi,
        FieldValue val, SetOpt setOpt) throws RedisResponseException {
        TranslateResult tr;
        if (setOpt == SetOpt.NX) {
            // Optimization for NX case: there is no reason to go though
            // an update statement because we are not in danger of overwriting
            // a key with wrong (non-json) type.
            // We can't do the same for no setOpt case, because we could
            // potentially overwrite a non-json key, so we would really need
            // to do read-modify-write with doGetSet(). Instead we do update
            // statement in hope that the key exists only perform another
            // request (doSet) if it doesn't.
            if (pi.isRoot) {
                return doSet(keyInfo, makeJSONValue(val), NO_EXP, true);
            }

            // Note that SQL_UPDATE_PUT_NX_FMT uses array filter step
            // expression in the PUT clause. Because of this, we have to make
            // sure it is not applied if the context item itself is an array,
            // otherwise this array would be converted to a sequence and our
            // parent path will include each element of this array which would
            // give wrong result. Since parent of new fields can only be a
            // map, we use MAP_FILTER as parent filter.
            // Another solution would be to remove array filter step
            // expression from SQL_UPDATE_PUT_NX_FMT and put it into the parent
            // filter, this would avoid using 2 parent filters. This would
            // require some more refactoring of the code since the parent
            // filter itself the result of the first visitor invocation.
            tr = translatePathWithFilter(pi, null, MAP_FILTER);
            if (tr.leafFields == null) {
                throw new RedisResponseException(
                    "cannot create new fields with this path");
            }
        } else {
            tr = translatePath(pi);
        }

        String sql = DECL_KEY_ID_VAL + tr.getSQLDecl() +
            (tr.leafFields == null || setOpt == SetOpt.XX ?
                String.format(SQL_UPDATE_SET_FMT, tr.sqlPath, tr.sqlPath) :
                (setOpt != SetOpt.NX ?
                    String.format(SQL_UPDATE_PUT_FMT, tr.parentSQLPath,
                        tr.getSQLNewFieldsExpr(), tr.sqlPath) :
                    String.format(SQL_UPDATE_PUT_NX_FMT, tr.sqlPath,
                        ((TranslateResultWithFilters)tr)
                            .parentSQLPathWithFilter(),
                    tr.sqlPath, tr.getSQLNewFieldsExpr(), tr.sqlPath)));
        
        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        pStmt.setVariable(SQL_VAL, val);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            if (setOpt == SetOpt.XX) {
                return false;
            }

            if (!pi.isRoot) {
                throw new RedisResponseException(ERR_NEW_VAL_NOT_ROOT);
            }

            assert setOpt == null; // we handled other cases above
            if (!doSet(keyInfo, makeJSONValue(val), NO_EXP, false)) {
                // This is rare case when another client just concurrently
                // inserted a new key.
                throw new RedisRetryException();
            };

            return true;
        }
        
        chkIsJSON(row);
        return getBoolRes(row);
    }

    private MapValue makeMultiResult(List<PathInfo> paths,
        ArrayValue vals) throws RedisResponseException {
        int cnt = paths.size();
        if (vals.size() != cnt) {
            throw RedisResponseException.corrupt(
                "JSON.GET: result count mismatch");
        }

        MapValue res = new MapValue(cnt);
        for(int i = 0; i < cnt; i++) {
            PathInfo pi = paths.get(i);
            FieldValue val = vals.get(i);
            if (!val.isArray()) {
                throw RedisResponseException.corrupt(ERR_NOT_ARRAY);
            }
            res.put(pi.origPath, chkLegacyRes(pi,
                untransformValues(val.asArray())));
        }

        return res;
    }

    private FieldValue doJSONGet(RedisKeyInfo keyInfo, List<PathInfo> paths)
        throws RedisResponseException {
        assert !paths.isEmpty();
        TranslateResult tr = paths.size() == 1 ?
            translatePath(paths.get(0)) : translatePaths(paths);
        String sql = String.format(SQL_GET_FMT, tr.getSQLDecl(), tr.sqlPath);

        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        
        MapValue row = doSQLGet(pStmt);
        if (row == null) {
            return null;
        }

        chkIsJSON(row);
        ArrayValue res = getArrRes(doSQLGet(pStmt));
        return paths.size() == 1 ?
            chkLegacyRes(paths.get(0), untransformValues(res)) :
            makeMultiResult(paths, res);
    }

    private boolean doJSONMSet(ByteBuf[] args) throws RedisResponseException {
        RedisKeyInfo[] keyInfos = makeRedisMultiKeyInfo(args, 0,
            args.length / 3, 3);

        HashMap<String, MapValue> byId = new HashMap<>();
        ArrayValue keyIds = makeKeyIdsValue(keyInfos);
        PreparedStatement pStmt = pstmtCache.getByRef(SQL_MSET_GET);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfos[0].slot));
        pStmt.setVariable(SQL_KEY_IDS, keyIds);
        processQuery(pStmt, row -> { byId.put(getId(row), row); });

        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        HashSet<String> chkDupSet = new HashSet<>();
        JSONPathValueVisitor visitor = new JSONPathValueVisitor();

        // Backward iteration to handle duplicate keys, same as for
        // StringCommands.handleMSet().
        for(int i = keyInfos.length - 1; i >= 0; i--) {
            RedisKeyInfo ki = keyInfos[i];
            if (!chkDupSet.add(ki.id)) {
                continue;
            }
            // Note that oldRow is query result in different format than
            // redis table row.
            MapValue oldRow = byId.get(ki.id);
            FieldValue oldVal = null;
            if (oldRow != null) {
                ki.exp = getExpTime(oldRow);
                if (!RedisValueInfo.isTimeExpired(ki.exp)) {
                    oldVal = oldRow.get("json");
                    if (oldVal == null) {
                        // Absence of "json" field in existing Redis key means
                        // it is not of type JSON.
                        throw RedisResponseException.wrongType();
                    }
                } else {
                    ki.exp = NO_EXP;
                }
            }

            PathInfo pi = PathInfo.get(args[i * 3 + 1]);
            FieldValue valToSet = byteBufToJson(args[i * 3 + 2]);

            FieldValue newVal;
            // We handle root path outside the visitor.
            if (pi.isRoot) {
                newVal = transformValue(valToSet);
            } else {
                if (oldVal == null) {
                    throw new RedisResponseException(ERR_NEW_VAL_NOT_ROOT);
                }
                // Because we store values in transformed form and the visitor
                // operates on untransformed values, we have to untransform and
                // then transform back.
                newVal = untransformValue(oldVal);
                ParseTree parseTree = parsePath(pi.path);
                // If we fail to set any values according to the provided path
                // (e.g. because parent field doesn't exist), there is no need
                // to update this row.
                if (!visitor.jsonSet(newVal, valToSet, parseTree, pi.path)) {
                    continue;
                };
                newVal = transformValue(newVal);
            }

            // We could've avoided conditional puts on root path since they
            // would be overwritten anyway. However, we have to preserve the
            // expiration times for existing keys since JSON.MSET keeps them.

            MapValue row = new MapValue().put(FLD_SLOT, ki.slot)
                .put(FLD_ID, ki.id).put(FLD_KEY, makeRedisKey(ki))
                // cmd.args alternate keys and values
                .put(FLD_VALUE, makeJSONValue(newVal));
            PutRequest putReq = new PutRequest()
                .setTableName(NoSQLRedisServer.MAIN_TABLE_NAME)
                .setValue(row);
            if (oldRow != null) {
                // Unlike MSET, JSON.MSET will keep expiration time for
                // existing keys. We only need to remove expiration time from
                // expired keys since they are treated as non-existent.
                if (oldVal == null) {
                    putReq.setTTL(TimeToLive.DO_NOT_EXPIRE);
                }

                // Make conditional on existing version.
                putReq.setOption(PutRequest.Option.IfVersion);
                putReq.setMatchVersion(rowToVer(oldRow));
            } else {
                putReq.setOption(PutRequest.Option.IfAbsent);
            }

            wmReq.add(putReq, true);
        }

        // Possible if provided paths did not update any values (see
        // visitor.jsonSet above).
        if (wmReq.getNumOperations() == 0) {
            return false;
        }

        try {
            WriteMultipleResult res = nosqlHandle.writeMultiple(wmReq);
            if (res.getSuccess()) {
                return true;
            }
            throw new RedisRetryException();
        } catch(NoSQLException ex) {
            throw RedisResponseException.nosql(ex);
        }
    }

    private boolean doJSONMerge(RedisKeyInfo keyInfo, PathInfo pi,
        FieldValue val) throws RedisResponseException {

        // If val is not an object, then the merge operation should just
        // replace the target with val. Particularly, we cannot use merge when
        // val = null, since if we do merge via parent path (see below), then
        // this would remove the target field(s) rather than assign null to it
        // as expected in this case.
        if (!val.isMap()) {
            return doJSONSet(keyInfo, pi, val, null);
        }
        
        // SQL merge patch does not allow us to merge with non-existing field.
        // This is similar situation to JSON.SET (see leafFields in
        // JSONPathToSQLVisitor). In this case we merge with the parent path
        // instead. E.g. to merge a.b.new_field with patch p, we merge a.b with
        // patch { new_field: p }. Note that this is only needed when
        // leafFields exist, since all other paths can only point to existing
        // values. However, we want to avoid cases when parent (e.g. a.b) is
        // not an object, since in this case merge on a.b.new_field should
        // fail, but would replace a.b if performed via parent path as above.

        String[] parentFilters = pi.isRoot ? null : MAP_FILTER_ARR;
        TranslateResultWithFilters tr = translatePathWithFilters(pi, null,
            parentFilters);
        String sql = String.format(SQL_MERGE_FMT, tr.getSQLDecl(),
            tr.leafFields != null ? tr.parentSQLPathWithFilter() : tr.sqlPath,
            tr.leafFields != null ? tr.getSQLNewFieldsExpr() : SQL_VAL,
            tr.sqlPath);

        PreparedStatement pStmt = getPrepStmt(keyInfo, sql, tr);
        pStmt.setVariable(SQL_VAL, val);

        MapValue row = doSQLUpdate(pStmt);
        if (row == null) {
            if (!pi.isRoot) {
                throw new RedisResponseException(ERR_NEW_VAL_NOT_ROOT);
            }

            // Merge to non-existing key is just assigning the patch to it.
            if (!doSet(keyInfo, makeJSONValue(val), NO_EXP, true)) {
                throw new RedisRetryException();
            };

            return true;
        }
        
        chkIsJSON(row);
        return getBoolRes(row);
    }

    public void registerCommands(HashMap<String, CommandHandler> cmdMap) {
        cmdMap.put(CMD_JSON_SET, this::handleJSONSet);
        cmdMap.put(CMD_JSON_GET, this::handleJSONGet);
        cmdMap.put(CMD_JSON_MGET, this::handleJSONMGet);
        cmdMap.put(CMD_JSON_MSET, this::handleJSONMSet);
        cmdMap.put(CMD_JSON_MERGE, this::handleJSONMerge);
    }

    public RedisMessage handleJSONSet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkNumArgs(cmd, 3, 4);
        
        PathInfo pi = PathInfo.get(cmd.args[1]);
        FieldValue val = transformValue(byteBufToJson(cmd.args[2]));
        SetOpt setOptArg = null;

        if (cmd.args.length == 4) {
            String arg = Utils.byteBufToString(cmd.args[3]);
            if (arg.equalsIgnoreCase("NX")) {
                chkNotSet(setOptArg);
                setOptArg = SetOpt.NX;
            } else if (arg.equalsIgnoreCase("XX")) {
                chkNotSet(setOptArg);
                setOptArg = SetOpt.XX;
            } else {
                throw RedisResponseException.syntaxError();
            }
        }

        final SetOpt setOpt = setOptArg;
        return doWithRetries(() ->
            doJSONSet(makeRedisKeyInfo(cmd.args[0]), pi, val, setOpt)) ?
            okReply : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    public RedisMessage handleJSONGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 1);
        
        ArrayList<PathInfo> paths = new ArrayList<>();
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
                paths.add(PathInfo.get(arg));
            }
        }

        if (paths.isEmpty()) {
            paths.add(PathInfo.LEGACY_ROOT);
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

    public RedisMessage handleJSONMGet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkMinNumArgs(cmd, 2);
        RedisKeyInfo[] keyInfos = makeRedisMultiKeyInfo(cmd.args, 0,
            cmd.args.length - 1);
        ArrayValue keyIds = makeKeyIdsValue(keyInfos);
        PathInfo pi = PathInfo.get(cmd.args[cmd.args.length - 1]);

        TranslateResult tr = translatePath(pi);
        String sql = String.format(SQL_MGET_FMT, tr.getSQLDecl(),
            tr.sqlPath);

        PreparedStatement pStmt = pstmtCache.getByVal(sql);
        pStmt.setVariable(SQL_SLOT, new IntegerValue(keyInfos[0].slot));
        pStmt.setVariable(SQL_KEY_IDS, keyIds);
        tr.vars.forEach(
            (varName, varVal) -> pStmt.setVariable(varName, varVal));
        
        // We need to return results for all provided keys in order, but the
        // query results may be in different order and/or missing non-existent
        // keys or keys of wrong type, so we need to collect query results
        // first to create the final result.
        HashMap<String,FieldValue> resMap = new HashMap<>();
        processQuery(pStmt, row -> {
            resMap.put(getId(row), chkLegacyRes(pi,
                untransformValues(getArrRes(row))));
        });

        ArrayList<RedisMessage> res = new ArrayList<>();
        for(FieldValue keyId: keyIds) {
            FieldValue keyRes = resMap.get(keyId.getString());
            res.add(keyRes != null ?
                new FullBulkStringRedisMessage(Utils.stringToByteBuf(
                    keyRes.toJson(null))) :
                FullBulkStringRedisMessage.NULL_INSTANCE);
        }

        return new ArrayRedisMessage(res);
    }

    public RedisMessage handleJSONMSet(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        if (cmd.args == null || cmd.args.length < 3 ||
            cmd.args.length % 3 != 0) {
            throw RedisResponseException.numArgs(cmd.name);
        }

        if (cmd.args.length > 3 * MAX_WM_CNT) {
            throw new RedisResponseException(
                RedisResponseException.ErrorPrefix.ERR,
                ERR_TOO_MANY_KEYS);
        }

        doWithRetries(() -> doJSONMSet(cmd.args));
        // We follow Redis spec and behavior that returns "OK" result even if
        // no values were set because of non-existent parent paths, although
        // it would make more sense to return more informative result.
        return okReply;
    }

    public RedisMessage handleJSONMerge(RedisClientContext client,
        RawCommand cmd) throws RedisResponseException {
        chkExactNumArgs(cmd, 3);
        PathInfo pi = PathInfo.get(cmd.args[1]);
        FieldValue val = transformValue(byteBufToJson(cmd.args[2]));

        return doWithRetries(() ->
            doJSONMerge(makeRedisKeyInfo(cmd.args[0]), pi, val)) ?
                okReply : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

}
