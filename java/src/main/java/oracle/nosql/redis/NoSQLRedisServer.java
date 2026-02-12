/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
 *     NoSQLRedisServer redisSvr = new NoSQLRedisServer(
 *         new RedisServerConfig(nosqlConfig));
 *     redisSvr.start();
 *     ...
 *     redisSvr.stop(60000);
 * </pre>
 * It is recommended to arrange the calls to {@link #start()} and
 * {@link #stop(long)} with your application lifecycle. Note that you can
 * start and stop {@link NoSQLRedisServer} instance only once. If a restart is
 * needed, create a new instance.
 */
public class NoSQLRedisServer {
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5000;

    /**
     * @hidden
     */
    public static final String MAIN_TABLE_NAME = "redis";

    /**
     * @hidden
     */
    public static final String CREATE_MAIN_TABLE =
        "CREATE TABLE IF NOT EXISTS redis(slot INTEGER, id STRING, " +
        "key JSON, value JSON, PRIMARY KEY(SHARD(slot), id))";

    /**
     * @hidden
     */
    public static final String CREATE_SCANID_IDX =
        "CREATE INDEX IF NOT EXISTS scanIdIdx ON redis(key.scanId AS LONG)";

    /**
     * @hidden
     */
    public static final String CREATE_LIST_TABLE =
        "CREATE TABLE IF NOT EXISTS redis.lists(elemId NUMBER, " +
        "cid STRING AS UUID, value STRING, PRIMARY KEY(elemId))";

    /**
     * @hidden
     */
    public static final String CREATE_HASH_TABLE =
        "CREATE TABLE IF NOT EXISTS redis.hashes(keyId STRING, " +
        "cid STRING AS UUID, key JSON, value STRING, PRIMARY KEY(keyId))";

    /**
     * @hidden
     */
    public static final String CREATE_HSCANID_IDX =
        "CREATE INDEX IF NOT EXISTS hScanIdIdx ON " +
        "redis.hashes(slot, id, key.scanId AS LONG)";

    private enum State {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED;

        @Override
        public String toString() {
            return name().replaceAll("_", " ").toLowerCase();
        }
    }
    
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final ChannelGroup clientChannels =
        new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final RedisServerConfig config;
    private final NoSQLHandle nosqlHandle;
    private final CommandHandlers cmdHandlers;
    private final ExecutorService cmdWorkerPool =
        Executors.newCachedThreadPool();
    // will use config in logging.properties if provided
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    private volatile Channel serverChannel;
    private volatile State state = State.NOT_STARTED;

    /**
     * Creates new instance of NoSQLRedisServer.
     * @param config Configuration object that specifies the parameters used
     * to start NoSQL Redis proxy.
     */
    public NoSQLRedisServer(RedisServerConfig config) {
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

    // Only used in main().
    private void waitForStop() {
        assert serverChannel != null;
        serverChannel.closeFuture().syncUninterruptibly();
    }

    private void shutdownGroups() {
        serverBootstrap.config().group().shutdownGracefully(0, 5,
            TimeUnit.SECONDS).awaitUninterruptibly();
        serverBootstrap.config().childGroup().shutdownGracefully(0, 5,
            TimeUnit.SECONDS).awaitUninterruptibly();
    }

    // Only used in main().
    private void doShutdown() {
        try {
            stop(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
        } catch (InterruptedException ex) {
            throw new IllegalStateException(
                "Redis Proxy main thread was interrupted");
        } catch (IllegalStateException ex) {
            // Thrown in case we attempt to shut down before the proxy has
            // been started, in which case we probably don't need to do
            // anything. We can log this error.
            logger.warning("Attempted to shutdown Redis Proxy before it has " +
                "fully started: " + ex.getMessage());
        }
    }

    /**
     * Starts NoSQL Redis proxy.
     * This method simply returns if the proxy is already running.
     * @throws IllegalStateException if the proxy is in state "starting",
     * "stopping" or "stopped" so that this call is invalid
     * @throws RuntimeException if the proxy failed to start for any other
     * reason
     */
    public void start() {
        synchronized (this) {
            if (state == State.RUNNING) {
                return;
            }
            if (state != State.NOT_STARTED) {
                throw new IllegalStateException(String.format(
                    "Cannot start Redis Proxy because it is '%s'",
                    state.toString()));
            }
            state = State.STARTING;
        }

        logger.info("Initializing table schema...");
        initDB();
        logger.info("Table schema initialized");

        try {
            assert serverChannel == null;
            serverChannel = serverBootstrap.bind(config.getHost(),
                    config.getPort())
                .syncUninterruptibly().channel();
            state = State.RUNNING;
        } catch (Throwable ex) {
            shutdownGroups();
            throw ex;
        }

        logger.info(String.format("Started Redis Proxy on %s:%d",
            config.getHost(), config.getPort()));
    }

    /**
     * Stops NoSQL Redis proxy.
     * This method simply returns if the proxy is already stopped. This method
     * is idempotent and can be called again if previous call return
     * {@code false} due to a timeout reached.
     * @param timeoutMillis timeout in milliseconds to wait for the proxy to
     * stop. Value {@code 0} means to wait forever.
     * @return {@code true} if the proxy was stopped successfully or the proxy
     * was not running, {@code false} if failed to stop the proxy within the
     * specified timeout
     * @throws InterruptedException if the current thread was interrupted
     * @throws RuntimeException if the proxy failed to stop for any other
     * reason
     */
    public boolean stop(long timeoutMillis) throws InterruptedException {
        synchronized (this) {
            logger.info("Stopping Redis Proxy...");
            if (state == State.STOPPED) {
                return true;
            }
            if (state != State.RUNNING && state != State.STOPPING) {
                throw new IllegalStateException(String.format(
                    "Cannot stop Redis Proxy because it is '%s'",
                    state.toString()));
            }
            state = State.STOPPING;
        }

        cmdWorkerPool.shutdown();
        if (!cmdWorkerPool.awaitTermination(
            timeoutMillis != 0 ? timeoutMillis : Long.MAX_VALUE,
            TimeUnit.MILLISECONDS)) {
            return false;
        }

        // try-catch in case the handle has already been closed
        try {
            nosqlHandle.close();
        } catch (IllegalStateException ex) {
        } catch (IllegalArgumentException ex) {
        }

        assert serverChannel != null;
        try {
            serverChannel.close().sync();
        } finally {
            shutdownGroups();
        }

        state = State.STOPPED;
        logger.info("Stopped Redis Proxy");
        return true;
    }

    /**
     * Stops NoSQL Redis proxy. This method is equivalent to {@code stop(0)}.
     * @throws InterruptedException if the current thread was interrupted
     * @throws RuntimeException if the proxy failed to stop for any other
     * reason
     */
    public synchronized void stop() throws InterruptedException {
        stop(0);
    }

    /**
     * Returns whether NoSQL Redis proxy is running.
     * This value is {@code false} if the proxy is not started or has been
     * stopped. It is also {@code false} if the proxy is in the process of
     * starting or stopping.
     * @return {@code true} if NoSQL Redis proxy is running, otherwise
     * {@code false}
     */
    public boolean isRunning() {
        return state == State.RUNNING;
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
                //ex.printStackTrace(System.err);
                System.err.println(CommandLine.usage());
                System.exit(1);
            }

            NoSQLRedisServer svr = new NoSQLRedisServer(config);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                svr.doShutdown();
            }));

            svr.start();
            svr.waitForStop();
        } catch (Throwable ex) {
            System.err.println("Redis Proxy Server exited with error:");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
