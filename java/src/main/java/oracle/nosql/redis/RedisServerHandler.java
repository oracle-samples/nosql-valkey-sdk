/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import java.util.concurrent.ExecutorService;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import oracle.nosql.redis.CommandHandlers.CommandHandler;

class RedisServerHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private final CommandHandlers handlers;
    private final ExecutorService cmdWorkerPool;
    private final ChannelGroup clientChannels;
    private final RedisClientContext client;

    RedisServerHandler(CommandHandlers handlers,
        ExecutorService cmdWorkerPool, ChannelGroup clientChannels) {
        this.handlers = handlers;
        this.cmdWorkerPool = cmdWorkerPool;
        this.clientChannels = clientChannels;
        this.client = new RedisClientContext();
    }

    private void executeCommand(ChannelHandlerContext ctx, RedisMessage msg) {
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
        } catch(RuntimeException ex) {
            Throwable cause = ex.getCause();
            if (!(cause instanceof RedisResponseException)) {
                throw ex;
            }
            res = new ErrorRedisMessage(cause.getMessage());
        } finally {
            if (cmd != null) {
                cmd.releaseBuffers();
            }
            // Release msg because it was retained in channelRead0.
            ReferenceCountUtil.release(msg);
        }

        ChannelFuture f = ctx.writeAndFlush(res);

        if (cmd.name.equals("QUIT")) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, RedisMessage msg)
        throws Exception {
        // msg will be released when channelRead0 exits, so we have to retain
        // it for execution in different thread.
        ReferenceCountUtil.retain(msg);
        cmdWorkerPool.execute(() -> executeCommand(ctx, msg));
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // This happens when redis-cli disconnects (even via exit command).
        // Todo: log the error.
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(RedisClientContext.ATTR_KEY).set(client);
        clientChannels.add(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(RedisClientContext.ATTR_KEY).set(null);
        clientChannels.remove(ctx.channel());
        super.channelInactive(ctx);
    }
    
}
