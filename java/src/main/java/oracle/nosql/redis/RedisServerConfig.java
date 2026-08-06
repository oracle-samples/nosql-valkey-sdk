/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.ops.TableLimits;

/**
 * RedisServerConfig specifies configuration parameters needed to start NoSQL
 * Valkey API Proxy. The instances of this class are immutable. The only required
 * configuration parameter is NoSQLHandleConfig instance that specifies the
 * configuration needed to connect to Oracle NoSQL Database. For other
 * parameters, if not specified, the default values are used. See the public
 * constants declared in this class.
 */
public class RedisServerConfig {

    private static final String ENV_IN_CONTAINER =
        "NOSQL_REDIS_PROXY_IN_CONTAINER";

    static final String DEFAULT_NOSQL_ENDPOINT = "localhost:8080";

    static final String DEFAULT_NOSQL_ENDPOINT_IN_CONTAINER =
        "host.docker.internal:8080";

    /**
     * Default table limits used to create main Redis table.
     * They are: 100 read units, 100 write units and 5 GB of storage.
     */
    public static final TableLimits DEFAULT_TABLE_LIMITS =
        new TableLimits(100, 100, 5);

    /**
     * Default hostname used to listen for client connections.
     */
    public static final String DEFAULT_HOST = "127.0.0.1";

    /**
     * localhost or 127.0.0.1 would only map to the localhost of the container,
     * not the host machine. We have to listen on all interfaces in order for
     * the proxy to be accessible from the host machine via port mapping.
     * @hidden
     */
    public static final String DEFAULT_HOST_IN_CONTAINER = "0.0.0.0";

    /**
     * Default port used to listen for client connections, same value as in
     * Redis Server.
     */
    public static final int DEFAULT_PORT = 6379;

    /**
     * Default limit on the number of retries of certain operations if
     * version mismatch is detected due to concurrent operation on the same
     * key.
     */
    public static final int DEFAULT_MAX_ATOMIC_RETRIES = 100;

    /**
     * Default value determining whether, on Valkey api proxy startup, to run a
     * background cleanup thread that will check for and purge any abandoned
     * collection element data that was left due to previous abnormal
     * termination. The default is {@code true}.
     */
    public static final boolean DEFAULT_CLEANUP_ELEMS_TABLES_ON_STARTUP = true;

    private final NoSQLHandleConfig nosqlConfig;
    private final TableLimits tableLimits;
    private final String host;
    private final int port;
    private final int maxAtomicRetries;
    private final boolean cleanupElemsTablesOnStartup;

    /**
     * Initializes an instance of RedisServerConfig with all configuration
     * parameters.
     * @param nosqlConfig configuration used to create NoSQLHandle that
     * represents connection to Oracle NoSQL database.
     * @param tableLimits table limits used to create main Redis table. If not
     * specified, the defaults will be used. When the table already exists and
     * this parameter is specified, it is used to update the table limits of
     * the table.
     * @param host hostname or ip address on which the Valkey api proxy will listen
     * for client connections.
     * @param port port number on which the Valkey api proxy will listen for client
     * connections.
     * @param maxAtomicRetries the limit on the number of retries of certain
     * operations if version mismatch is detected due to concurrent operation
     * on the same key. After the limit is reached, an error will be returned
     * to the application.
     * @param cleanupElemsTablesOnStartup whether, on Valkey api proxy startup, to
     * run a background cleanup thread that will check for and purge any
     * abandoned collection element data that was left due to previous
     * abnormal termination.
     */
    public RedisServerConfig(NoSQLHandleConfig nosqlConfig,
        TableLimits tableLimits, String host, int port, int maxAtomicRetries,
        boolean cleanupElemsTablesOnStartup)
    {
        this.nosqlConfig = nosqlConfig;
        this.tableLimits = tableLimits;
        this.host = host;
        this.port = port;
        this.maxAtomicRetries = maxAtomicRetries;
        this.cleanupElemsTablesOnStartup = cleanupElemsTablesOnStartup;
    }

    /**
     * Initializes an instance of RedisServerConfig with specified values for
     * {@code nosqlConfig} and {@code tableLimits} and default values for
     * other parameters.
     * @param nosqlConfig configuration used to create NoSQLHandle that
     * represents connection to Oracle NoSQL database.
     * @param tableLimits table limits used to create main Redis table. Has
     * effect only when connecting for the first time when the database schema
     * is created.
     * @see #RedisServerConfig(NoSQLHandleConfig, TableLimits, String, int,
     * int, boolean)
     */
    public RedisServerConfig(NoSQLHandleConfig nosqlConfig,
        TableLimits tableLimits) {
        this(nosqlConfig, tableLimits, DEFAULT_HOST, DEFAULT_PORT,
            DEFAULT_MAX_ATOMIC_RETRIES,
            DEFAULT_CLEANUP_ELEMS_TABLES_ON_STARTUP);
    }

    /**
     * Initializes an instance of RedisServerConfig with specified value for
     * {@code nosqlConfig} and default values for other parameters.
     * @param nosqlConfig configuration used to create NoSQLHandle that
     * represents connection to Oracle NoSQL database.
     * @see #RedisServerConfig(NoSQLHandleConfig, TableLimits, String, int,
     * int, boolean)
     */
    public RedisServerConfig(NoSQLHandleConfig nosqlConfig) {
        this(nosqlConfig, null);
    }

    /**
     * @hidden
     */
    public static boolean isRunningInContainer() {
        String val = System.getenv(ENV_IN_CONTAINER);
        return val != null && Boolean.parseBoolean(val);
    }

    /**
     * Returns configuration used to create NoSQLHandle that represents
     * connection to Oracle NoSQL database.
     * @return configuration used to create NoSQLHandle
     */
    public NoSQLHandleConfig getNosqlConfig() {
        return nosqlConfig;
    }

    /**
     * Returns table limits used to create main Redis table.
     * @return table limits for main Redis table
     */
    public TableLimits getTableLimits() {
        return tableLimits;
    }


    /**
     * Returns hostname or ip address on which the Valkey api proxy listens for
     * client connections.
     * @return host name or ip address
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns port on which Valkey api proxy listens for client connections.
     * @return port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the limit on the number of retries of certain operations if
     * version mismatch is detected due to concurrent operation on the same
     * key.
     * @return limit on number of retries
     */
    public int getMaxAtomicRetries() {
        return maxAtomicRetries;
    }

    /**
     * Returns whether, on Valkey api proxy startup, to run a background cleanup
     * thread that will check for and purge any abandoned collection element
     * data that was left due to previous abnormal termination.
     * @return {@code true} to run background cleanup thread on startup,
     * otherwise {@code false}
     */
    public boolean getCleanupElemsTablesOnStartup() {
        return cleanupElemsTablesOnStartup;
    }

}
