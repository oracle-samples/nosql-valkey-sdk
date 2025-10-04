/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;

public class NoSQLRedisServer {
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5000;

    public static final String MAIN_TABLE_NAME = "redis";
    public static final String CREATE_MAIN_TABLE =
        "CREATE TABLE IF NOT EXISTS redis(slot INTEGER, id STRING, " +
        "key JSON, value JSON, PRIMARY KEY(SHARD(slot), id))";
    public static final String CREATE_SCANID_IDX =
        "CREATE INDEX IF NOT EXISTS scanIdIdx ON redis(key.scanId AS LONG)";
    public static final String CREATE_LIST_TABLE =
        "CREATE TABLE IF NOT EXISTS redis.lists(elemId NUMBER, " +
        "cid STRING AS UUID, value STRING, PRIMARY KEY(elemId))";
    public static final String CREATE_LIST_PK2_IDX =
        "CREATE INDEX IF NOT EXISTS listPK2Idx ON " +
        "redis.lists(slot, id, elemId)";
    public static final String CREATE_LISTID_IDX =
        "CREATE INDEX IF NOT EXISTS listIdIdx ON redis.lists(cid)";
    public static final String CREATE_HASH_TABLE =
        "CREATE TABLE IF NOT EXISTS redis.hashes(keyId STRING, " +
        "cid STRING AS UUID, key JSON, value STRING, PRIMARY KEY(keyId))";
    public static final String CREATE_HASHID_IDX =
        "CREATE INDEX IF NOT EXISTS hashIdIdx ON redis.hashes(cid)";
    public static final String CREATE_HSCANID_IDX =
        "CREATE INDEX IF NOT EXISTS hScanIdIdx ON " +
        "redis.hashes(key.scanId AS LONG)";
    
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private Channel serverChannel;
    private ChannelGroup clientChannels =
        new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final RedisServerConfig config;
    private final NoSQLHandle nosqlHandle;
    private final CommandHandlers cmdHandlers;
    private final ExecutorService cmdWorkerPool =
        Executors.newCachedThreadPool();
    private Thread svrRun;
    private Exception svrRunEx;

    public NoSQLRedisServer(RedisServerConfig config) {
        this.config = config;
        this.nosqlHandle = NoSQLHandleFactory.createNoSQLHandle(
            config.nosqlConfig);
        
        cmdHandlers = new CommandHandlers(nosqlHandle, config, clientChannels);
        cmdHandlers.init();

        initDB();

        EventLoopGroup bossGroup = new NioEventLoopGroup();
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
                    p.addLast(new RedisServerHandler(cmdHandlers,
                        cmdWorkerPool, clientChannels));
                }
            });
    }

    private void initDB() {
        nosqlHandle.doTableRequest(new TableRequest()
                .setStatement(CREATE_MAIN_TABLE)
                .setTableLimits(config.tableLimits),
            30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_SCANID_IDX), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_LIST_TABLE), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_LIST_PK2_IDX), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_LISTID_IDX), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_HASH_TABLE), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_HASHID_IDX), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_HSCANID_IDX), 30000, 500);

        if (config.cleanupElemsTablesOnStartup) {
            cmdHandlers.scheduleElemTablesCleanup(cmdWorkerPool);
        }
    }

    private void run() {
        try {
            // Start the server.
            serverChannel = serverBootstrap.bind(config.host, config.port)
                .syncUninterruptibly().channel();
            // Wait until the server socket is closed.
            serverChannel.closeFuture().syncUninterruptibly();
        } finally {
            // Shut down all event loops to terminate all threads.
            serverBootstrap.config().group().shutdownGracefully();
            serverBootstrap.config().childGroup().shutdownGracefully();
        }
    }

    private boolean close(long timeoutMillis) {
        ChannelFuture f = serverChannel.close();
        if (timeoutMillis == 0) {
            f.syncUninterruptibly();
            return true;
        }

        if (!f.awaitUninterruptibly(timeoutMillis)) {
            return false;
        }

        if (!f.isSuccess()) {
            Throwable t = f.cause();
            throw new RuntimeException(t);
        }

        return true;
    }

    public synchronized boolean isRunning() {
        return svrRun != null && svrRunEx == null;
    }

    public synchronized Exception getException() {
        return svrRunEx;
    }

    public synchronized void start() {
        if (isRunning()) {
            throw new IllegalStateException(
                "Redis proxy server is already running");
        }

        svrRunEx = null;
        svrRun = new Thread(() -> {
            try {
                run();
            } catch (Exception ex) {
                svrRunEx = ex;
                // log the error
            }
        });

        svrRun.start();
    }

    public synchronized boolean stop(long timeoutMillis)
        throws InterruptedException {
        if (!isRunning()) {
            throw new IllegalStateException(
                "Redis proxy server is not running");
        }

        if (timeoutMillis == 0) {
            close(0);
            svrRun.join();
            svrRunEx = null;
            return true;
        }

        long startTime = System.currentTimeMillis();
        if (!close(timeoutMillis)) {
            return false;
        }

        svrRun.join(timeoutMillis - (System.currentTimeMillis() - startTime));
        if (svrRun.isAlive()) {
            return false;
        }

        svrRun = null;
        return true;
    }

    public synchronized void stop() throws InterruptedException {
        stop(0);
    }

    public static void main(String[] args) {
        try {
            RedisServerConfig config = null;
            try {
                CommandLine cmdLine = new CommandLine(args);
                config = cmdLine.getRedisServerConfig();
            } catch (IllegalArgumentException ex) {
                System.err.println(ex.getMessage());
                System.err.println(CommandLine.usage());
                System.exit(1);
            }

            NoSQLRedisServer svr = new NoSQLRedisServer(config);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                svr.close(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
            }));

            svr.run();
        } catch (Throwable ex) {
            System.err.println("Redis Proxy Server exited with error:");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
