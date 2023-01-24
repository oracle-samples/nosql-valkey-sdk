/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
 package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.PreparedStatementCache;
import oracle.nosql.redis.util.Utils;

class Scan extends CommandsBase {
	
    private static final String SCAN_STMT =
        "SELECT t.key, t.value.type AS type FROM redis t "
        + "WHERE t.key.scanId >= ? AND (NOT EXISTS t.key.exp OR "
        + "t.key.exp >= ?) ORDER BY t.key.scanId";

    private static final String FLD_TYPE = VALUE_TYPE;

    private static final String REGEX_ESCAPE_CHARS = "<([{\\^-=$!|]})?*+.>";

    Scan(NoSQLHandle nosqlHandle, PreparedStatementCache pstmtCache) {
        super(nosqlHandle, pstmtCache);
    }

    private static long getScanId(MapValue key)
        throws RedisResponseException {
        FieldValue scanId = key.get(KEY_SCAN_ID);
        if (scanId == null || !scanId.isLong()) {
            throw RedisResponseException.corrupt(
                "Missing or invalid scan id");
        }
        return scanId.getLong();
    }

    private static ByteBuf keyToKeyBuf(MapValue key)
        throws RedisResponseException {
        String data = getData(key);
        boolean isBin = getIsBin(key);
        ByteBuf buf = Unpooled.copiedBuffer(data, CharsetUtil.UTF_8);
        return isBin ? Base64.decode(buf) : buf;
    }

    private static String rowToType(MapValue row)
        throws RedisResponseException {
        FieldValue type = row.get(FLD_TYPE);
        if (type == null || !type.isString()) {
            throw RedisResponseException.corrupt(
                "Invalid value type");
        }
        return type.getString();
    }

    // It is not clear what to do if the glob pattern and/or the input is
    // not UTF-8, since we can only operate with strings.  For now we do the
    // decoding, which should use CodingErrorAction.REPLACE on decoding error.
    // My guess is that Redis allows binary glob matches (although it is not
    // mentioned in the docs).
    // Todo: we could allow binary matches by escaping non-UTF8 portion of
    // both the pattern and the input in such a way as to preserve the
    // matching behavior.
    private static String byteBufToString(ByteBuf buf) {
        return buf.toString(CharsetUtil.UTF_8);
    }

    private static void appendCharToRegex(StringBuilder buf, char c) {
        if (REGEX_ESCAPE_CHARS.indexOf(c) >= 0) {
            buf.append('\\');
        }
        buf.append(c);
    }

    // This is not the fastest algorithm, but easier to undestand than using
    // quoted/unquoted regions.  The glob format used by Redis is not
    // documented in great detail, so the behavior is partially based on
    // experimentation with Redis server.
    // The glob format used by Redis inside brackets ([]) does not seem to
    // conform to any known glob or regex format (see comments below).
    // One feature not supported below is reverse ranges, e.g. [z-a] (even
    // though Redis does support it by inverting the range e.g. to [a-z]).
    // If needed, this will require more work (considering various special
    // cases).
    // It is possible we may decide to go with existing Glob format used by
    // java.nio.file.PathMatcher.  However, this presents its own problems
    // because this format is different from Redis and more suited to match
    // file system paths.
    private static Pattern globToRegex(String glob) {
        StringBuilder res = new StringBuilder();
        res.append("(?s)^");
        boolean isEscape = false;
        boolean inBrackets = false;
        int lastDash = -2;
        for(int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (isEscape) {
                appendCharToRegex(res, c);
                isEscape = false;
                continue;
            }
            // '\' is used as escape for any character following it, unless
            // '\' is last, in which case it is interpreted as literal.
            if (c == '\\' && i < glob.length() - 1) {
                isEscape = true;
                continue;
            }
            if (!inBrackets) {
                switch(c) {
                    case '*':
                        res.append(".*");
                        break;
                    case '?':
                        res.append('.');
                        break;
                    case '[':
                        inBrackets = true;
                        res.append('[');
                        // Special cases right after '['.
                        char c1 = i < glob.length() - 1 ?
                            glob.charAt(i + 1) : ']';
                        // "[]" matches nothing, we simulate it with Java
                        // regex [^\s\S].
                        if (c1 == ']') {
                            res.append("^\\s\\S");
                            break;
                        }
                        // '^' has its regex meaning only if it immediately
                        // follows the 1st '[', otherwise it is interpreted as
                        // a literal.
                        if (c1 == '^') {
                            // "[^]" matches any character, we simulate it
                            // with Java regex [\s\S].
                            if (i == glob.length() - 2 ||
                                glob.charAt(i + 2) == ']') {
                                res.append("\\s\\S");
                            } else {
                                res.append('^');
                            }
                            i++;
                            break;
                        }
                        // '-' is interpreted literally if it immediately
                        // follows the 1st '[', otherwise it has the same
                        // meaning as in regex.
                        if (c1 == '-') {
                            appendCharToRegex(res, '-');
                            i++;
                        }
                        break;
                    default:
                        appendCharToRegex(res, c);
                }
            } else if (c == '-') {
                res.append('-');  // regex meaning of '-' inside brackets
                lastDash = i;
            } else if (c == ']') {
                // Only outer brackets have their meaning as in regex.  Nested
                // '[' is interpreted literally (e.g. "[[]" matches "]" and
                // "[[]]" matches "[]").
                // ']' closes the brackets unless preceded by '-', in which
                // case ']' is interpreted as a literal.
                if (lastDash != i - 1) {
                    inBrackets = false;
                    res.append(']'); // closing brackets
                } else {
                    appendCharToRegex(res, ']');
                }
            } else {
                appendCharToRegex(res, c);
            }
        }
        // Redis completes open bracket if not closed at the end.
        if (inBrackets) {
            res.append(']');
        }
        res.append('$');
        return Pattern.compile(res.toString());
    }

    private static int getLimit(long count) {
        return (int)Math.min(count + 1, Integer.MAX_VALUE);
    }

    // This will need to be reconsidered.  String.hashCode() is not very
    // strong anyway.  Need to look for better 3rd party hash function.
    // Make sure the scan id != 0, because of special meaning of cursor = 0.
    static long makeScanId(RedisKeyInfo keyInfo) {
        long vLow = keyInfo.id.hashCode();
        long vHigh = new StringBuilder(keyInfo.id).reverse().toString()
            .hashCode();
        long res = (vHigh << 32) | vLow;
        // This should not really be possible (hashcode = 0 for both string
        // and its reverse?), but just in case.
        if (res == 0) {
            res = Long.MIN_VALUE;
        }
        return res;
    }

    // Returns next cursor value.  Accumulates results into results list.
    long scan(long cursor, ByteBuf match, long count, String type,
        List<RedisMessage> results) throws RedisResponseException {
        Pattern matchPattern = null;
        if (match != null) {
            try {
                matchPattern = globToRegex(byteBufToString(match));
            } catch(PatternSyntaxException ex) {
                // Todo: log the exception.  Not all patterns may be currently
                // supported, so we notify the user.
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "unsupported glob pattern");
            }
        }

        PreparedStatement pStmt = pstmtCache.get(SCAN_STMT)
            .setVariable(1, new LongValue(
                cursor == 0 ? Long.MIN_VALUE : cursor))
            .setVariable(2, new LongValue(System.currentTimeMillis()));
        // Nested "try" to avoid resource leak warning on qReq because of
        // set... methods below.
        try(QueryRequest qReq = new QueryRequest()) {
            qReq.setPreparedStatement(pStmt).setLimit(getLimit(count));
            try(QueryIterableResult qir = nosqlHandle.queryIterable(qReq)) {
                long rowCnt = 0;
                long currScanId = 0;
                long scanId = 0;

                for(MapValue row : qir) {
                    rowCnt++;
                    MapValue key = rowToKey(row);
                    scanId = getScanId(key);

                    // We stop only if the scanId changes (or there are no
                    // more rows). This will ensure that that we retrieved all
                    // keys with given scanId, so that the cursor can advance
                    // for the next SCAN invocation.
                    if (scanId != currScanId) {
                        if (rowCnt > count) {
                            break;
                        }
                        currScanId = scanId;
                    }
                    
                    if (type != null && !type.equalsIgnoreCase(
                        rowToType(row))) {
                        continue;
                    }

                    ByteBuf keyBuf = keyToKeyBuf(key);
                    if (matchPattern != null && !matchPattern.matcher(
                        byteBufToString(keyBuf)).matches()) {
                        continue;
                    }
                    
                    results.add(new FullBulkStringRedisMessage(keyBuf));
                }

                // If there are no more rows, we are done.
                if (scanId == currScanId) {
                    scanId = 0;
                }

                return scanId;
            }
        }  
    }

    // Returns result in the format of the result of SCAN command.
    RedisMessage scan(long cursor, ByteBuf match, long count, String type)
        throws RedisResponseException {
        ArrayList<RedisMessage> keyList = new ArrayList<>();
        long nextCursor = scan(cursor, match, count, type, keyList);

        ArrayList<RedisMessage> resList = new ArrayList<>(2);
        resList.add(new FullBulkStringRedisMessage(
            Utils.longToByteBuf(nextCursor)));
        resList.add(new ArrayRedisMessage(keyList));
        return new ArrayRedisMessage(resList);
    }
}
