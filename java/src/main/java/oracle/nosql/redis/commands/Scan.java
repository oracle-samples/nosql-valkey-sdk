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
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingPredicate;

abstract class Scan extends CommandsBase implements AutoCloseable {
    
    private static final String REGEX_ESCAPE_CHARS = "<([{\\^-=$!|]})?*+.>";

    static final long DEFAULT_COUNT = 10;

    Scan(CommandsBase cmds) {
        super(cmds.nosqlHandle, cmds.config, cmds.pstmtCache);
    }

    private static int getLimit(long count) {
        return (int)Math.min(count + 1, Integer.MAX_VALUE);
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

    static long getScanId(MapValue key) throws RedisResponseException {
        FieldValue scanId = key.get(KEY_SCAN_ID);
        if (scanId == null || !scanId.isLong()) {
            throw RedisResponseException.corrupt(
                "Missing or invalid scan id");
        }
        return scanId.getLong();
    }

    static long rowToScanIdUnchecked(MapValue row) {
        return row.get(FLD_KEY).asMap().getLong(KEY_SCAN_ID);
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

    abstract Iterable<MapValue> scanIterable(RedisKeyInfo keyInfo,
        long cursor, int limit) throws RedisResponseException;
    
    // Some scans may add more than one RedisMessage for each row, e.g. HSCAN
    // adds key and value for each hash entry. The subclasses may need to
    // override this method.
    void addResults(List<RedisMessage> results, ByteBuf keyBuf,
        MapValue row) throws RedisResponseException {
        results.add(new FullBulkStringRedisMessage(keyBuf));
    }

    // Returns next cursor value.  Accumulates results into results list.
    // pred is an optional additional predicate to test candidate row (in
    // addition to match), currently used only for "TYPE" parameter to SCAN.
    long scan(RedisKeyInfo keyInfo, long cursor, ByteBuf match, long count,
        ThrowingPredicate<MapValue,RedisResponseException> pred,
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

        Iterable<MapValue> resIter = null;
        try {
            resIter = scanIterable(keyInfo,
                cursor == 0 ? Long.MIN_VALUE : cursor, getLimit(count));

            long rowCnt = 0;
            long currScanId = 0;
            long scanId = 0;

            for(MapValue row : resIter) {
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
                
                if (pred != null && !pred.test(row)) {
                    continue;
                }

                ByteBuf keyBuf = keyToKeyBuf(key);
                if (matchPattern != null && !matchPattern.matcher(
                    byteBufToString(keyBuf)).matches()) {
                    continue;
                }
                
                addResults(results, keyBuf, row);
            }

            // If there are no more rows, we are done.
            if (scanId == currScanId) {
                scanId = 0;
            }

            return scanId;
        } finally {
            if (resIter instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)resIter).close();
                } catch(Exception ex) {
                    assert false : "resIter.close() should not throw";
                }
            }
        }
    }

    // Returns result in the format of the result of SCAN command.
    RedisMessage scan(RedisKeyInfo keyInfo, long cursor, ByteBuf match,
        long count, ThrowingPredicate<MapValue,RedisResponseException> pred)
        throws RedisResponseException {
        ArrayList<RedisMessage> keyList = new ArrayList<>();
        long nextCursor = scan(keyInfo, cursor, match, count, pred, keyList);

        ArrayList<RedisMessage> resList = new ArrayList<>(2);
        resList.add(new FullBulkStringRedisMessage(
            Utils.longToByteBuf(nextCursor)));
        resList.add(new ArrayRedisMessage(keyList));
        return new ArrayRedisMessage(resList);
    }

    public void close() {}    
}
