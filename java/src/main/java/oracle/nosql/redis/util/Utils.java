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
import oracle.nosql.redis.RedisResponseException;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;

public class Utils {
    
    @FunctionalInterface
    public interface ThrowingFunction<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    @FunctionalInterface
    public interface ThrowingBiFunction<T1, T2, R, E extends Exception> {
        R apply(T1 t1, T2 t2) throws E;
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final String SHA256_ALG = "SHA-256";

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

}
