/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
 
package oracle.nosql.redis.commands;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.Utils;
import oracle.nosql.redis.util.Utils.ThrowingPredicate;

abstract class Scan extends CommandsBase implements AutoCloseable {
    
    private static final String REGEX_ESCAPE_CHARS = "<([{\\^-=$!|]})?*+.>";

    static final int DEFAULT_COUNT = 10;
    static final int MAX_COUNT = 100000000;

    // Key info for HSCAN, SSCAN, ZSCAN, not used for SCAN.
    final protected RedisKeyInfo keyInfo;
    final protected long startScanId;
    final protected Pattern matchPattern;
    // An optional additional predicate to test candidate row (in addition to
    // matchPatter), currently used only for "TYPE" parameter to SCAN.
    final protected ThrowingPredicate<MapValue,RedisResponseException>
        valuePred;
    final protected int count;
    // To know when cursor = 0 is passed. In this case we set startScanId to
    // Long.MIN_VALUE to cover the whole scanId range.
    final protected boolean zeroCursor;

    Scan(CommandsBase cmds, RedisKeyInfo keyInfo, long cursor, ByteBuf match,
        long count, ThrowingPredicate<MapValue,RedisResponseException> pred)
        throws RedisResponseException {
        super(cmds.nosqlHandle, cmds.config, cmds.pstmtCache);
        this.keyInfo = keyInfo;

        if (cursor == 0) {
            zeroCursor = true;
            startScanId = Long.MIN_VALUE;
        } else {
            zeroCursor = false;
            startScanId = cursor;
        }

        valuePred = pred;
        this.count = (int)Math.min(count, MAX_COUNT);

        if (match == null) {
            matchPattern = null;
        } else {
            try {
                matchPattern = globToRegex(byteBufToString(match));
            } catch(PatternSyntaxException ex) {
                // Todo: log the exception.  Not all patterns may be currently
                // supported, so we notify the user.
                throw new RedisResponseException(ErrorPrefix.ERR,
                    "unsupported glob pattern");
            }
        }
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

    // This is not the fastest algorithm, but easier to understand than using
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
        return Utils.getLongField(key, KEY_SCAN_ID);
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

    // Creates an iterable that iterates through records containing fields
    // "key" and "value". The key is in the same format as stored in the parent
    // table (see CommandsBase.makeRedisKey()), scanId is required if
    // toGetAll() = false. The value is a binary string.
    abstract Iterator<MapValue> startScan()
        throws RedisResponseException;

    // Override this for cases where we want to get all keys regardless of
    // count. This is usually when small collection is stored entirely in the
    // parent table record (e.g. smallVal for hashes).
    boolean toGetAll() { return false; }

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
    long scan(List<RedisMessage> results) throws RedisResponseException {
        Iterator<MapValue> resIter = startScan();

        long rowCnt = 0;
        long currScanId = 0;
        long scanId = 0;

        while(resIter.hasNext()) {
            MapValue row = resIter.next();
            rowCnt++;
            MapValue key = rowToKey(row);

            if (!toGetAll()) {
                scanId = getScanId(key);

                // We stop only if the scanId changes (or there are no
                // more rows). This will ensure that we retrieved all
                // keys with given scanId, so that the cursor can advance
                // for the next SCAN invocation.
                if (scanId != currScanId) {
                    if (rowCnt > count) {
                        break;
                    }
                    currScanId = scanId;
                }
            }

            if (valuePred != null && !valuePred.test(row)) {
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
    }

    // Returns result in the format of the result of SCAN command.
    RedisMessage scan()
        throws RedisResponseException {
        ArrayList<RedisMessage> keyList = new ArrayList<>();
        long nextCursor = scan(keyList);

        ArrayList<RedisMessage> resList = new ArrayList<>(2);
        resList.add(new FullBulkStringRedisMessage(
            Utils.longToByteBuf(nextCursor)));
        resList.add(new ArrayRedisMessage(keyList));
        return new ArrayRedisMessage(resList);
    }

    public void close() {}    
}
