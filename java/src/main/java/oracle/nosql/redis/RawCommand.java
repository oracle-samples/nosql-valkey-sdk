/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.InlineCommandRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.CharsetUtil;
import oracle.nosql.redis.RedisResponseException.ErrorPrefix;
import oracle.nosql.redis.util.Utils;

public class RawCommand {
	
    public final String name;
    public final ByteBuf[] args;

    RawCommand(RedisMessage msg) throws RedisResponseException {
        String cmdName = null;
        if (msg instanceof ArrayRedisMessage) {
            List<RedisMessage> elems = ((ArrayRedisMessage)msg)
                .children();
            int cnt = elems.size();
            if (cnt == 0) {
                throw new RedisResponseException(ErrorPrefix.PROTOCOL,
                    "Empty command array");
            }
            args = cnt > 1 ? new ByteBuf[cnt - 1] : null;
            for(int i = 0; i < cnt; i++) {
                RedisMessage elem = elems.get(i);
                if (!(elem instanceof FullBulkStringRedisMessage)) {
                    throw new RedisResponseException(ErrorPrefix.PROTOCOL,
                        "Command must be an array of bulk strings");
                }
                ByteBuf buf = ((FullBulkStringRedisMessage)elem).content();
                if (i == 0) {
                    cmdName = Utils.byteBufToString(buf);
                } else {
                    assert args != null;
                    buf.retain();
                    args[i - 1] = buf;
                }
            }
        } else if (msg instanceof InlineCommandRedisMessage) {
            String[] elems = ((InlineCommandRedisMessage)msg).content()
                .split("\\s+");            
            if (elems.length == 0) {
                throw new RedisResponseException(ErrorPrefix.PROTOCOL,
                    "Empty command list");
            }
            cmdName = elems[0];
            args = elems.length > 1 ? new ByteBuf[elems.length - 1] : null;
            for(int i = 1; i < elems.length; i++) {
                args[i - 1] = Unpooled.wrappedBuffer(elems[i].getBytes(
                    CharsetUtil.UTF_8));
            }
        } else {
            throw new RedisResponseException(ErrorPrefix.PROTOCOL,
                "Command must be an array of bulk strings or " +
                "an inline command");
        }

        assert cmdName != null;
        name = cmdName.toUpperCase();
    }

    void releaseBuffers() {
        if (args != null) {
            for(ByteBuf arg : args) {
                arg.release();
            }
        }
    }

}
