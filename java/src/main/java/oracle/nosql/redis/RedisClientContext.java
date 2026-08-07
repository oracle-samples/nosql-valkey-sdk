/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey;

import io.netty.util.AttributeKey;

public class RedisClientContext {

    public static final AttributeKey<RedisClientContext> ATTR_KEY =
        AttributeKey.valueOf("oracle.nosql.redis.RedisClientContext");

    private volatile boolean isBlocked;

    public boolean isBlocked() { return isBlocked; }

    public void setBlocked(boolean isBlocked) {
        this.isBlocked = isBlocked;
    }
}
