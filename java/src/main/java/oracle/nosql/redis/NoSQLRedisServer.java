/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.redis.util.Utils;

/**
 * This class represents NoSQL Redis proxy. You can use it to run the proxy
 * within your application's process. For example:
 * <pre>
 *     import oracle.nosql.driver.NoSQLHandleConfig;
 *     import oracle.nosql.driver.iam.SignatureProvider;
 *     ...
 *     NoSQLHandleConfig nosqlConfig = new NoSQLHandleConfig(
 *       Region.US_PHOENIX_1);
 *     nosqlConfig.setDefaultCompartment(...);
 *     nosqlConfig.setAuthorizationProvider(new SignatureProvider(...));
 *     NoSQLRedisServer redisSvr = NoSQLRedisServer.startServer(
 *         new RedisServerConfig(nosqlConfig));
 *     ...
 *     redisSvr.stop(60000);
 * </pre>
 * It is recommended to arrange the calls to
 * {@link #startServer(RedisServerConfig)} and {@link #stop(long)} with your
 * application lifecycle. Note that you cannot restart a stopped
 * {@link NoSQLRedisServer} instance. To start again, call
 * {@link #startServer(RedisServerConfig)} to create a new instance.
 */
public class NoSQLRedisServer {
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5000;
    private static final int HARD_TIMEOUT_MILLIS = 500;

    /**
     * @hidden
     */
    public static final String MAIN_TABLE_NAME = "redis";

    /**
     * @hidden
     */
    public static final String CREATE_MAIN_TABLE =
        "CREATE TABLE IF NOT EXISTS valkey(slot INTEGER, id STRING, " +
        "key JSON, value JSON, PRIMARY KEY(SHARD(slot), id))";

    /**
     * @hidden
     */
    public static final String CREATE_SCANID_IDX =
        "CREATE INDEX IF NOT EXISTS scanIdIdx ON valkey(key.scanId AS LONG)";

    /**
     * @hidden
     */
    public static final String CREATE_LIST_TABLE =
        "CREATE TABLE IF NOT EXISTS valkey.lists(elemId NUMBER, " +
        "cid STRING AS UUID, value STRING, PRIMARY KEY(elemId))";

    /**
     * @hidden
     */
    public static final String CREATE_HASH_TABLE =
        "CREATE TABLE IF NOT EXISTS valkey.hashes(keyId STRING, " +
        "cid STRING AS UUID, key JSON, value STRING, PRIMARY KEY(keyId))";

    /**
     * @hidden
     */
    public static final String CREATE_HSCANID_IDX =
        "CREATE INDEX IF NOT EXISTS hScanIdIdx ON " +
        "valkey.hashes(slot, id, key.scanId AS LONG)";

    // will use config in logging.properties if provided
    private static final Logger logger =
        Logger.getLogger(NoSQLRedisServer.class.getName());

    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final ChannelGroup clientChannels =
        new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final RedisServerConfig config;
    private final NoSQLHandle nosqlHandle;
    private final CommandHandlers cmdHandlers;
    private final ExecutorService cmdWorkerPool =
        Executors.newCachedThreadPool();
    private volatile Channel serverChannel;
    private volatile boolean stopped = false;

    private NoSQLRedisServer(RedisServerConfig config) {
        this.config = config;
        this.nosqlHandle = NoSQLHandleFactory.createNoSQLHandle(
            config.getNosqlConfig());
        
        cmdHandlers = new CommandHandlers(nosqlHandle, config, clientChannels);
        cmdHandlers.init();

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
        TableLimits tableLimits = config.getTableLimits();
        TableResult res = nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_MAIN_TABLE)
            .setTableLimits(tableLimits != null ?
                tableLimits : RedisServerConfig.DEFAULT_TABLE_LIMITS),
            30000, 500);
        if (res.getTableState() != TableResult.State.ACTIVE) {
            throw new IllegalStateException("Main table state is not ACTIVE");
        }
        // If table limits are provided in config that are different from the
        // table limits of the existing table, we send another request to
        // update the table limits. This is ignored for on-prem
        // (when res.getTableLimits() is null).
        if (res.getTableLimits() != null && tableLimits != null &&
            !Utils.tableLimitsEqual(res.getTableLimits(), tableLimits)) {
            nosqlHandle.doTableRequest(new TableRequest()
                .setTableName(MAIN_TABLE_NAME)
                .setTableLimits(tableLimits), 30000, 500);
        }

        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_SCANID_IDX), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_LIST_TABLE), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_HASH_TABLE), 30000, 500);
        nosqlHandle.doTableRequest(new TableRequest()
            .setStatement(CREATE_HSCANID_IDX), 30000, 500);

        if (config.getCleanupElemsTablesOnStartup()) {
            cmdHandlers.scheduleElemTablesCleanup(cmdWorkerPool);
        }
    }

    private void start() {
        logger.info("Initializing table schema...");
        initDB();
        logger.info("Table schema initialized");

        try {
            assert serverChannel == null;
            serverChannel = serverBootstrap.bind(config.getHost(),
                    config.getPort())
                .syncUninterruptibly().channel();
        } catch (Exception ex) {
            hardStop();
            throw ex;
        }

        logger.info(String.format("Started Redis Proxy on %s:%d",
            config.getHost(), config.getPort()));
    }

    // Only used in main().
    private void waitForStop() {
        assert serverChannel != null;
        serverChannel.closeFuture().syncUninterruptibly();
    }

    private boolean softStop(long timeoutMillis) throws InterruptedException {
        long endTime = System.currentTimeMillis() + timeoutMillis;
        if (serverChannel != null && !serverChannel.close().await(
            timeoutMillis, TimeUnit.MILLISECONDS)) {
            return false;
        }

        timeoutMillis = Math.max(endTime - System.currentTimeMillis(), 0);
        if (!serverBootstrap.config().childGroup().shutdownGracefully(0, 5,
            TimeUnit.SECONDS).await(timeoutMillis)) {
            return false;
        };
        timeoutMillis = Math.max(endTime - System.currentTimeMillis(), 0);
        if (!serverBootstrap.config().group().shutdownGracefully(0, 5,
            TimeUnit.SECONDS).await(timeoutMillis)) {
            return false;
        };

        cmdWorkerPool.shutdown();
        timeoutMillis = Math.max(endTime - System.currentTimeMillis(), 0);
        if (!cmdWorkerPool.awaitTermination(timeoutMillis,
            TimeUnit.MILLISECONDS)) {
            return false;
        }

        // NoSQLHandle.close() is not idempotent, so we have to catch
        // exception in case close() is called more than once.
        try {
            nosqlHandle.close();
        } catch (IllegalArgumentException ex) {
        } catch (IllegalStateException ex) {
        }

        stopped = true;
        return true;
    }

    private void hardStop() {
        if (serverChannel != null) {
            try {
                if (!serverChannel.close()
                    .awaitUninterruptibly(HARD_TIMEOUT_MILLIS)) {
                    System.err.println("Timeout closing server channel");
                }
            } catch (Exception ex) {
                System.err.println("Exception closing server channel: " + ex);
            }
        }

        if (!serverBootstrap.config().childGroup().shutdownGracefully(
            0, HARD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            .awaitUninterruptibly(HARD_TIMEOUT_MILLIS)) {
            System.err.println(
                "Timeout shutting down netty server bootstrap worker group");
        };
        if (!serverBootstrap.config().group().shutdownGracefully(0,
            HARD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            .awaitUninterruptibly(HARD_TIMEOUT_MILLIS)) {
            System.err.println(
                "Timeout shutting down netty server bootstrap boss group");
        };

        cmdWorkerPool.shutdownNow();
        try {
            if (!cmdWorkerPool.awaitTermination(HARD_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS)) {
                System.err.println(
                    "Timeout waiting to shut down Redis proxy worker pool");
            }
        } catch (InterruptedException ex) {
            System.err.println(
                "Waiting for shut down of Redis proxy worker pool was " +
                "interrupted");
        }

        // NoSQLHandle.close() is not idempotent, so we have to catch
        // exception in case close() is called more than once.
        try {
            nosqlHandle.close();
        } catch (IllegalArgumentException ex) {
        } catch (IllegalStateException ex) {
        } catch (Exception ex) {
            System.err.println("Exception closing NoSQLHandle " + ex);
        }

        stopped = true;
    }

    // Note that logging does not work in shutdown hook because LogManager adds
    // its own shutdown hook and shutdown hooks are executed in unspecified
    // order. I found that available workarounds are not reliable and will not
    // work in all cases. Thus, will use regular console output or error
    // streams.
    private void shutdownHook() {
        try {
            stop();
        } catch (Exception ex) {
            // We can ignore it here for now since stop() will already print
            // the error messages.
        }
    }

    /**
     * Creates and starts NoSQL Redis proxy.
     * @param config Configuration object that specifies the parameters used
     * to start NoSQL Redis proxy.
     * @return New instance of {@link NoSQLRedisServer} representing running
     * Redis proxy
     * @throws RuntimeException if the Redis proxy failed to start, in which
     * case the resources are released and the exception is rethrown back to
     * the caller
     */
    public static NoSQLRedisServer startServer(RedisServerConfig config) {
        NoSQLRedisServer svr = new NoSQLRedisServer(config);
        svr.start();
        return svr;
    }

    /**
     * Stops NoSQL Redis proxy.
     * <p>
     * This method tries to perform a soft shutdown within the specified
     * timeout, which allows currently executing commands to finish. If soft
     * shutdown times out or an exception is thrown during soft shutdown, hard
     * shutdown is performed to release the resources on the best effort basis.
     * Timeouts and exceptions during soft and shutdowns are logged to stderr,
     * with any exception from soft shutdown also rethrown back to the caller.
     * </p>
     * This method simply returns {@code true} if the proxy is already stopped.
     * @param timeoutMillis timeout in milliseconds to perform soft shutdown
     * @return {@code true} if the soft shutdown was successful or the proxy
     * was already stopped, {@code false} if soft shutdown timed out and hard
     * shutdown had to be performed
     * @throws IllegalArgumentException if the timeout is negative
     * @throws InterruptedException if the current thread was interrupted
     * @throws RuntimeException if the soft shutdown failed for any other
     * reason
     */
    public boolean stop(long timeoutMillis) throws InterruptedException {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeout must not be negative");
        }

        // There is a race condition here but doStop() is idempotent, so we can
        // ignore it.
        if (stopped) {
            return true;
        }

        // This method is used in shutdown hook, thus not using logger.
        System.out.println("\nStopping Redis Proxy...");

        try {
            if (!softStop(timeoutMillis)) {
                System.err.println(
                    "Soft stop of Redis proxy timed out after " +
                    timeoutMillis + "ms, performing hard stop...");
                hardStop();
                return false;
            };
        } catch (Exception ex) {
            System.err.println("Exception during soft stop of Redis Proxy: " +
                ex);
            System.err.println("Performing hard stop...");
            hardStop();
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw ex;
        }

        System.out.println("Stopped Redis Proxy");
        return true;
    }

    /**
     * Stops NoSQL Redis proxy using default timeout of five seconds. This
     * method behaves the same as {@link #stop(long)} and performs hard
     * shutdown if soft shutdown timed out or failed for another reason.
     * @throws InterruptedException if the current thread was interrupted
     * @throws RuntimeException if soft shutdown failed for any other reason
     */
    public void stop() throws InterruptedException {
        stop(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    /**
     * Returns whether NoSQL Redis proxy is stopped. You cannot reuse stopped
     * proxy. Call {@link #startServer(RedisServerConfig)} to create a new
     * instance.
     * @return {@code true} if NoSQL Redis proxy is stopped, otherwise
     * {@code false}
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * @hidden
     */
    public static void main(String[] args) {
        try {
            RedisServerConfig config = null;
            try {
                CommandLine cmdLine = new CommandLine(args);
                config = cmdLine.getRedisServerConfig();
            } catch (IllegalArgumentException ex) {
                System.err.println(ex.getMessage());
                Throwable cause = ex.getCause();
                if (cause != null) {
                    System.err.println("Caused by: " + cause.getMessage());
                }
                System.err.println(CommandLine.usage());
                System.exit(1);
            }

            NoSQLRedisServer svr = NoSQLRedisServer.startServer(config);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                svr.shutdownHook();
            }));
            svr.waitForStop();
        } catch (Throwable ex) {
            System.err.println("Redis Proxy Server exited with error:");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
