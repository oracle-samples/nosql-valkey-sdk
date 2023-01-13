/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import oracle.nosql.redis.CommandHandlers.CommandHandler;

class RedisServerHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private CommandHandlers handlers;
    private RedisClientContext client;

    RedisServerHandler(CommandHandlers handlers) {
        this.handlers = handlers;
        this.client = new RedisClientContext();
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, RedisMessage msg)
        throws Exception {
        RawCommand cmd = null;
        RedisMessage res;
        try {
            cmd = new RawCommand(msg);
            CommandHandler handler = handlers.getHandler(cmd.name);
            if (handler == null) {
                throw RedisResponseException.unknownCommand(cmd);
            }
            res = handler.apply(client, cmd);
        } catch(RedisResponseException rrex) {
            res = new ErrorRedisMessage(rrex.getMessage());
        } finally {
            if (cmd != null) {
                cmd.releaseBuffers();
            }
        }

        ChannelFuture f = ctx.writeAndFlush(res);

        if (cmd.name == "QUIT") {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.print("exceptionCaught: ");
        cause.printStackTrace(System.err);
        ctx.close();
    }
    
}
