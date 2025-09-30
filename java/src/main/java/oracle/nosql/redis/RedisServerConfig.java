/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.ops.TableLimits;

public class RedisServerConfig {

    private static final String ENV_IN_CONTAINER =
        "NOSQL_REDIS_PROXY_IN_CONTAINER";

    public static final String DEFAULT_NOSQL_ENDPOINT = "localhost:8080";
    public static final TableLimits DEFAULT_TABLE_LIMITS =
        new TableLimits(100, 100, 5);
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final String DEFAULT_HOST_IN_CONTAINER = "0.0.0.0";
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_MAX_ATOMIC_RETRIES = 100;
    public static final boolean DEFAULT_CLEANUP_ELEMS_TABLES_ON_STARTUP = true;

    public final NoSQLHandleConfig nosqlConfig;

    public final TableLimits tableLimits;

    public String host;

    public int port;

    public final int maxAtomicRetries;

    public final boolean cleanupElemsTablesOnStartup;

    RedisServerConfig(NoSQLHandleConfig nosqlConfig, TableLimits tableLimits,
        String host, int port, int maxAtomicRetries,
        boolean cleanupElemsTablesOnStartup)
    {
        this.nosqlConfig = nosqlConfig;
        this.tableLimits = tableLimits;
        this.host = host;
        this.port = port;
        this.maxAtomicRetries = maxAtomicRetries;
        this.cleanupElemsTablesOnStartup = cleanupElemsTablesOnStartup;
    }

    public static boolean isRunningInContainer() {
        String val = System.getenv(ENV_IN_CONTAINER);
        return val != null && Boolean.parseBoolean(val);
    }

}
