/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.util;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PreparedStatement;

public class PreparedStatementCache {
	
    private final NoSQLHandle handle;

    public PreparedStatementCache(NoSQLHandle handle)
    {
        this.handle = handle;
    }

    public PreparedStatement getPreparedStatement(String stmt) {
        return null;
    }

}
