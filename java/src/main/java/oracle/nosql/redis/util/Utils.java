/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.util;

import java.security.MessageDigest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.CharsetUtil;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;

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

    private static RedisResponseException missingOrInvalidField(String name,
        String type) {
        return RedisResponseException.corrupt(String.format(
            "Missing or invalid %s field %s", type, name));
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

    public static long byteBufToLong(ByteBuf buf)
        throws RedisResponseException {
        String str = byteBufToString(buf);
        try {
            return Long.parseLong(str);
        } catch(NumberFormatException ex) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "value is not an integer or out of range");
        }
    }

    public static ByteBuf longToByteBuf(long val) {
        return stringToByteBuf(Long.toString(val));
    }

    public static double byteBufToDouble(ByteBuf buf)
        throws RedisResponseException {
        String str = byteBufToString(buf);
        try {
            return Double.parseDouble(str);
        } catch(NumberFormatException ex) {
            throw new RedisResponseException(ErrorPrefix.ERR,
                "value is not a valid float");
        }
    }

    public static ByteBuf doubleToByteBuf(double val) {
        return stringToByteBuf(Double.toString(val));
    }

    // It looks like Redis rounds to the nearest second.
    public static long millisToSeconds(long millis) {
        return Math.round((double)millis / 1000);
    }

    public static String createDigest(ByteBuf buf) {
        try {
            MessageDigest digest = MessageDigest.getInstance(SHA256_ALG);
            byte[] res = digest.digest(ByteBufUtil.getBytes(buf, 0,
                buf.readableBytes(), false));
            return Base64.encode(Unpooled.wrappedBuffer(res)).toString(
                CharsetUtil.UTF_8);
        } catch(Exception ex) {
            // This shouldn't happen.
            assert false;
            throw new RuntimeException("Unexpected message digest error", ex);
        }
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
    
    // TODO: need to integrate this with other functions that do retries in
    // CommandsBase.java and CollectionCommandsBase.java.
    // TODO: introduce parameters or other methods that do exponential backoff.
    public static <R> R doWithRetries(
        ThrowingNoArgFunction<R, RedisResponseException> func, int numRetries)
        throws RedisResponseException {
        for(int i = 0; i < numRetries; i++) {
            try {
                return func.apply();
            } catch(RedisRetryException ex) {
                // retry
            }
        }
        throw new RedisResponseException(ErrorPrefix.NOSQL,
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
        return res.getValue() > 0 ? res : TimeToLive.ofHours(1);
    }

}
