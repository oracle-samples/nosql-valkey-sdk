/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.redis.util.CommandLine;

public class NoSQLRedisServer {
    private static final String DEFAULT_HOST =
    	System.getProperty("host", "127.0.0.1");
    private static final int DEFAULT_PORT =
    	Integer.parseInt(System.getProperty("port", "6379"));

    public static final String MAIN_TABLE_NAME = "redis";
    public static final String CREATE_MAIN_TABLE =
        "CREATE TABLE IF NOT EXISTS redis(id STRING, key JSON, value JSON, " +
        "PRIMARY KEY(id))";
    public static final String CREATE_SCANID_IDX =
        "CREATE INDEX IF NOT EXISTS scanIdIdx ON redis(key.scanId AS LONG)";
    private static final TableLimits DEFAULT_TABLE_LIMITS =
        new TableLimits(20000, 20000, 10);
    private static final TableLimits CLOUDSIM_TABLE_LIMITS =
        new TableLimits(2000000, 2000000, 10);
    public static final String CREATE_LIST_TABLE =
        "CREATE TABLE IF NOT EXISTS redis.lists(elemId NUMBER, " +
        "value STRING, PRIMARY KEY(elemId))";
    
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private Channel serverChannel;
    private final String host;
    private final int port;
    private final NoSQLHandle nosqlHandle;
    private final CommandHandlers cmdHandlers;
    private static boolean useCloudSim;

    private void initDB() {
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_MAIN_TABLE)
            /*
            .setTableLimits(
                useCloudSim ? CLOUDSIM_TABLE_LIMITS : DEFAULT_TABLE_LIMITS),
            */
            .setTableLimits(DEFAULT_TABLE_LIMITS),
            30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_SCANID_IDX), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_LIST_TABLE), 30000, 500);
    }

    public NoSQLRedisServer(String host, int port, NoSQLHandle nosqlHandle) {
        this.host = host;
        this.port = port;
        this.nosqlHandle = nosqlHandle;
        this.cmdHandlers = new CommandHandlers(nosqlHandle);

        initDB();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        serverBootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch)
                    throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new RedisDecoder(true));
                    p.addLast(new RedisBulkStringAggregator());
                    p.addLast(new RedisArrayAggregator());
                    p.addLast(new RedisEncoder());
                    p.addLast(new RedisServerHandler(cmdHandlers));
                }
            });
    }

    public NoSQLRedisServer(NoSQLHandle nosqlHandle) {
        this(DEFAULT_HOST, DEFAULT_PORT, nosqlHandle);
    }

    public void run() throws Exception {
        try {
            // Start the server.
            serverChannel = serverBootstrap.bind(host, port).sync().channel();
            // Wait until the server socket is closed.
            serverChannel.closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            serverBootstrap.config().group().shutdownGracefully();
            serverBootstrap.config().childGroup().shutdownGracefully();
        }
    }

    public void close() throws Exception {
        serverChannel.close().sync();
    }
    
    public static void main(String[] args) throws Exception {
        CommandLine cmdLine = new CommandLine(args);
        NoSQLHandleConfig nosqlConfig = new NoSQLHandleConfig(
            cmdLine.getEndpoint());
        useCloudSim = cmdLine.useCloudSim();
        nosqlConfig.setAuthorizationProvider(cmdLine.getAuthProvider());
        NoSQLHandle nosqlHandle = NoSQLHandleFactory.createNoSQLHandle(
            nosqlConfig);

        NoSQLRedisServer s = new NoSQLRedisServer(nosqlHandle);
        s.run();
    }

}
