/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.valkey.RedisResponseException;
import oracle.nosql.valkey.RedisResponseException.ErrorPrefix;

public class Utils {

    @FunctionalInterface
    public interface ThrowingNoArgFunction<R, E extends Exception> {
        R apply() throws E;
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    @FunctionalInterface
    public interface ThrowingBiFunction<T1, T2, R, E extends Exception> {
        R apply(T1 t1, T2 t2) throws E;
    }

    @FunctionalInterface
    public interface ThrowingTriFunction<T1, T2, T3, R, E extends Exception> {
        R apply(T1 t1, T2 t2, T3 t3) throws E;
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T, E extends Exception> {
        void accept(T t) throws E;
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface ThrowingPredicate<T, E extends Exception> {
        boolean test(T t) throws E;
    }

    public static class RedisRetryException extends RuntimeException {
    }

    private static final String SHA256_ALG = "SHA-256";

    // Taken from LOOKUP_TABLE in
    // https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/codec/CRC16.java
    private static final int[] CRC16_LOOKUP_TABLE = {
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
        0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
        0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
        0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
        0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
        0x48C4, 0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969, 0xA90A, 0xB92B,
        0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12,
        0xDBFD, 0xCBDC, 0xFBBF, 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A,
        0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
        0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
        0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
        0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
        0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
        0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
        0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
        0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
        0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
        0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
        0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
        0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
        0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
        0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
        0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0
    };

    // From WriteOptions.java in JE. We have to limit TTL to these values to
    // avoid errors.
    private static final int TTL_MAX_HOURS = Integer.MAX_VALUE / 2;
    private static final int TTL_MAX_DAYS = TTL_MAX_HOURS / 24;
    private static final int REDIS_FLOAT_MAX_SCALE = 17;

    private static final String ERR_INVALID_FLOAT =
        "value is not a valid float";

    private static final Base64.Encoder b64encoder = Base64.getEncoder();
    private static final Base64.Decoder b64decoder = Base64.getDecoder();

    private static final int START_RETRY_DELAY_MS = 4;
    private static final int MAX_RETRY_DELAY_MS = 2048;

    private static RedisResponseException missingOrInvalidField(String name,
        String type) {
        return RedisResponseException.corrupt(String.format(
            "Missing or invalid %s field %s", type, name));
    }

    // For the results represented in INCRBYFLOAT commands: must not use
    // exponent, also must have no more than 17 digits after decimal point and
    // must not have trailing zeroes.
    private static String redisFloatToString(double d) {
        // INCRBYFLOAT does not allow infinity/NaN, but we check here for
        // completeness.
        if (!Double.isFinite(d)) {
            return Double.toString(d);
        }
        BigDecimal bd = BigDecimal.valueOf(d);
        // Note that the below check is needed for very small values, e.g.
        // 1e-100, which will result in large number of digits after decimal
        // point.
        if (bd.scale() > REDIS_FLOAT_MAX_SCALE) {
            bd = bd.setScale(REDIS_FLOAT_MAX_SCALE, RoundingMode.HALF_UP);
        }
        return bd.stripTrailingZeros().toPlainString();
    }

    private static double parseRedisFloat(String val)
        throws RedisResponseException{
        // Unlike Double.parseDouble(), Redis does not allow numeric value
        // surrounded by whitespace or control chars < \u0020.
        // It seems that Long.parseLong() already does not allow whitespace so
        // we don't need to modify it.
        if (val.isEmpty() || val.charAt(0) <= 0x20 ||
            val.charAt(val.length() - 1) <= 0x20) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                ERR_INVALID_FLOAT);
        }
        try {
            return Double.parseDouble(val);
        } catch(NumberFormatException ex) {
            val = val.toLowerCase();
            // For hexadecimal format, Java requires binary exponent, but Redis
            // does not require it, so we handle the case with missing binary
            // exponent here.
            if (val.startsWith("0x") && val.indexOf('p') == -1) {
                try {
                    return Double.parseDouble(val + "p0");
                } catch(NumberFormatException ex2) {
                    throw new RedisResponseException(ErrorPrefix.ERR,
                        ERR_INVALID_FLOAT);
                }
            }

            // Redis parses values inf, +inf, -inf, +infinity, -infinity,
            // ignoring the case, as corresponding +/- infinity.
            if (val.equals("inf") || val.equals("infinity") ||
                val.equals("+inf") || val.equals("+infinity")) {
                return Double.POSITIVE_INFINITY;
            }
            if (val.equals("-inf") || val.equals("-infinity")) {
                return Double.NEGATIVE_INFINITY;
            }

            throw new RedisResponseException(ErrorPrefix.ERR,
                ERR_INVALID_FLOAT);
        }
    }

    // Using the result of io.netty.handler.codec.base64.Base64.decode() in
    // the pipeline produces a leak, the cause of which is unclear. For now,
    // switched to use java.util.Base64. For encode(), this is actually
    // better, because io.netty.handler.codec.base64.Base64 allocates direct
    // buffers which bears extra cost, and there is no reason for to have that
    // cost since we are not passing encoded buffers down the pipeline.
    // We can always switch base64 implementation by just changing the
    // functions below.

    public static String base64encode(byte[] bytes) {
        return b64encoder.encodeToString(bytes);
    }

    public static String base64encode(ByteBuf buf) {
        return b64encoder.encodeToString(
            ByteBufUtil.getBytes(buf, 0, buf.readableBytes(), false));
    }

    public static ByteBuf base64decode(String str) {
        return Unpooled.wrappedBuffer(b64decoder.decode(str));
    }

    public static String byteBufToString(ByteBuf buf)
        throws RedisResponseException {
        // Using CharsetDecoder with CodingErrorAction.REPORT might be more
        // efficient, but need to consider that CharsetDecoder is not
        // thread-safe.
        if (!ByteBufUtil.isText(buf, CharsetUtil.UTF_8)) {
            throw new RedisResponseException(ErrorPrefix.PROTOCOL,
                "Invalid UTF-8 string");
        }
        return buf.toString(CharsetUtil.UTF_8);
    }

    public static ByteBuf stringToByteBuf(String val) {
        return Unpooled.wrappedBuffer(val.getBytes(CharsetUtil.UTF_8));
    }

    public static long byteBufToLong(ByteBuf buf, String name)
        throws RedisResponseException {
        String str = byteBufToString(buf);
        try {
            return Long.parseLong(str);
        } catch(NumberFormatException ex) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                name + " is not an integer or out of range");
        }
    }

    public static long byteBufToLong(ByteBuf buf)
        throws RedisResponseException {
        return byteBufToLong(buf, "value");
    }

    public static ByteBuf longToByteBuf(long val) {
        return stringToByteBuf(Long.toString(val));
    }

    public static double byteBufToDouble(ByteBuf buf)
        throws RedisResponseException {
        return parseRedisFloat(byteBufToString(buf));
    }

    public static ByteBuf doubleToByteBuf(double val) {
        return stringToByteBuf(redisFloatToString(val));
    }

    // Used by INCRBY, INCR, DECR, HINCRBY.
    public static ByteBuf incrBy(ByteBuf val, long delta, boolean toAdd)
        throws RedisResponseException {
        long longVal = val != null ? Utils.byteBufToLong(val) : 0;
        long res;
        try {
            res = toAdd ?
                Math.addExact(longVal, delta) :
                Math.subtractExact(longVal, delta);
        } catch (ArithmeticException e) {
            throw new RedisResponseException(
                RedisResponseException.ErrorPrefix.ERR,
                "increment or decrement would overflow");
        }
        return Utils.longToByteBuf(res);
    }

    // Used by INCRBYFLOAT and HINCRBYFLOAT commands.
    public static ByteBuf incrByFloat(ByteBuf val, double delta)
        throws RedisResponseException {
        // To conform to Redis error response.
        if (Double.isNaN(delta)) {
            throw new RedisResponseException(ERR_INVALID_FLOAT);
        }
        double res = (val != null ? byteBufToDouble(val) : 0) + delta;
        if (!Double.isFinite(res)) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "increment would produce NaN or Infinity");
        }
        return doubleToByteBuf(res);
    }

    public static <T, R> Function<T,R> uncheckedFunc(
        ThrowingFunction<T, R, RedisResponseException> f) {
        return arg -> {
            try {
                return f.apply(arg);
            } catch (RedisResponseException ex) {
                throw RedisResponseException.unchecked(ex);
            }
        };
    }

    public static <T> Predicate<T> uncheckedPred(
        ThrowingPredicate<T, RedisResponseException> f) {
        return arg -> {
            try {
                return f.test(arg);
            } catch (RedisResponseException ex) {
                throw RedisResponseException.unchecked(ex);
            }
        };
    }

    // It looks like Redis rounds to the nearest second.
    public static long millisToSeconds(long millis) {
        return Math.round((double)millis / 1000);
    }

    public static String createDigest(ByteBuf buf) {
        try {
            MessageDigest digest = MessageDigest.getInstance(SHA256_ALG);
            byte[] res = digest.digest(ByteBufUtil.getBytes(
                buf, 0, buf.readableBytes(), false));
            return base64encode(res);
        } catch(Exception ex) {
            // This shouldn't happen.
            assert false;
            throw new RuntimeException("Unexpected message digest error", ex);
        }
    }

    // This is needed for Redis error messages. Redis error messages are
    // simple strings and are not allowed to contain '\n' or '\r'.
    public static String escapeSimpleString(String str) {
        if (str == null) {
            return "(null)";
        }

        return str.replace("\n", "\\n").replace("\r", "\\r");
    }

    // Used when by JSON Path parser and visitor.
    public static RuntimeException parseException(String input, int pos,
        String msg, Throwable cause) {
        // Sometimes position is reported past the end of the path string.
        int adjPos = Math.min(pos, input.length());
        return RedisResponseException.unchecked(
            new RedisResponseException(String.format(
                // Same format as in Redis Stack.
                "Error occurred on position %d, \"%s  ---->>>> %s\", %s",
                pos, input.substring(0, adjPos),
                input.substring(adjPos), msg), cause));
    }

    public static RuntimeException parseException(String input, int pos,
        String msg) {
        return parseException(input, pos, msg, null);
    }

    public static <R> R doWithRetries(
        ThrowingNoArgFunction<R, RedisResponseException> func, int numRetries)
        throws RedisResponseException {
        int delay = 0;
        for(int i = 0; i < numRetries; i++) {
            try {
                return func.apply();
            } catch(RedisRetryException ex) {
                // We do the first retry attempt without delay, since it will
                // most likely succeed unless in a highly concurrent
                // environment.
                if (delay == 0) {
                    delay = START_RETRY_DELAY_MS;
                } else {
                    // half-fixed/half-random delay
                    int currDelay = (delay / 2) +
                        (int)(Math.random() * (delay/ 2));
                    try {
                        Thread.sleep(currDelay);
                    } catch(InterruptedException ie) {
                        // The thread could get interrupted during hard stop
                        // of the proxy, in this case we should exit the loop.
                        // We restore the interrupt so that it is seen by
                        // Netty when writing a response.
                        Thread.currentThread().interrupt();
                        throw RedisResponseException.nosql(
                            "Atomic operation interrupted after " + (i + 1) +
                            " tries");
                    }
                    delay = Math.min(MAX_RETRY_DELAY_MS, delay * 2);
                }
                // retry
            }
        }
        throw RedisResponseException.nosql(
            "Failed to perform atomic operation after " + numRetries +
            " tries");
    }

    // Taken from crc16 in
    // https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/codec/CRC16.java
    public static int crc16(byte[] bytes, int off, int len) {
        int crc = 0;
        int end = off + len;

        for (int i = off; i < end; i++) {
            crc = (crc << 8) ^
                CRC16_LOOKUP_TABLE[((crc >>> 8) ^ (bytes[i] & 0xFF)) & 0xFF];
        }

        return crc & 0xFFFF;
    }

    public static int crc16(byte[] bytes) {
        return crc16(bytes, 0, bytes.length);
    }

    public static int crc16(ByteBuf buf) {
        return crc16(ByteBufUtil.getBytes(buf, 0, buf.readableBytes(), false));
    }

    public static String getStringField(MapValue mapVal, String fieldName,
        boolean allowNull) throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (allowNull && fldVal != null && fldVal.isAnyNull()) {
            return null;
        }
        if (fldVal == null || !fldVal.isString()) {
            throw missingOrInvalidField(fieldName, "string");
        }

        return fldVal.getString();
    }

    public static String getStringField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        return getStringField(mapVal, fieldName, false);
    }

    public static int getIntField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (fldVal == null || !fldVal.isInteger()) {
            throw missingOrInvalidField(fieldName, "integer");
        }

        return fldVal.getInt();
    }

    public static long getLongField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (fldVal == null || !fldVal.isLong()) {
            throw missingOrInvalidField(fieldName, "long");
        }

        return fldVal.getLong();
    }

    public static boolean getBoolField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (fldVal == null || !fldVal.isBoolean()) {
            throw missingOrInvalidField(fieldName, "boolean");
        }

        return fldVal.getBoolean();
    }

    public static ArrayValue getArrField(MapValue mapVal, String fieldName,
        boolean allowNull) throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (allowNull && fldVal != null && fldVal.isAnyNull()) {
            return null;
        }
        if (fldVal == null || !fldVal.isArray()) {
            throw missingOrInvalidField(fieldName, "array");
        }

        return fldVal.asArray();
    }

    public static ArrayValue getArrField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        return getArrField(mapVal, fieldName, false);
    }

    public static MapValue getMapField(MapValue mapVal, String fieldName,
        boolean allowNull) throws RedisResponseException {
        FieldValue fldVal = mapVal.get(fieldName);
        if (allowNull && fldVal != null && fldVal.isAnyNull()) {
            return null;
        }
        if (fldVal == null || !fldVal.isMap()) {
            throw missingOrInvalidField(fieldName, "map");
        }

        return fldVal.asMap();
    }

    public static MapValue getMapField(MapValue mapVal, String fieldName)
        throws RedisResponseException {
        return getMapField(mapVal, fieldName, false);
    }

    // Always returns positive TTL to be used for put. If expTime is already in
    // the past, return minimum TTL of 1 hour.
    public static TimeToLive getPositiveTTL(long expTime) {
        TimeToLive res = TimeToLive.fromExpirationTime(expTime,
            System.currentTimeMillis());

        // Limit to max values to avoid error from JE.
        if (res.getUnit() == TimeUnit.HOURS) {
            if (res.getValue() > TTL_MAX_HOURS) {
                res = TimeToLive.ofHours(TTL_MAX_HOURS);
            }
        } else if (res.getValue() > TTL_MAX_DAYS) {
            res = TimeToLive.ofDays(TTL_MAX_DAYS);
        }

        return res.getValue() > 0 ? res : TimeToLive.ofHours(1);
    }

    public static boolean tableLimitsEqual(TableLimits limits1,
        TableLimits limits2) {
        assert limits1 != null && limits2 != null;
        return limits1.getMode() == limits2.getMode() &&
            limits1.getReadUnits() == limits2.getReadUnits() &&
            limits1.getWriteUnits() == limits2.getWriteUnits() &&
            limits1.getStorageGB() == limits2.getStorageGB();
    }

    public static <T> Stream<T> iterableToStream(Iterable<T> it) {
        return StreamSupport.stream(it.spliterator(), false);
    }

    public static <T> Stream<T> iteratorToStream(Iterator<T> iter) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
            iter, Spliterator.ORDERED), false);
    }

}
