/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PreparedStatement;

public class PreparedStatementCache {
	
    private final NoSQLHandle handle;
    private Map<String,PreparedStatement> map;

    public PreparedStatementCache(NoSQLHandle handle)
    {
        this.handle = handle;
        map = Collections.synchronizedMap(new IdentityHashMap<>());
    }

    public PreparedStatement get(String stmt) {
        PreparedStatement pStmt = map.get(stmt);
        if (pStmt == null) {
            pStmt = handle.prepare(new PrepareRequest().setStatement(stmt))
                .getPreparedStatement();
            map.put(stmt, pStmt);
            return pStmt;
        }
        // How much faster would using ThreadLocal be rather than copying
        // every time?
        return pStmt.copyStatement();
    }

}
