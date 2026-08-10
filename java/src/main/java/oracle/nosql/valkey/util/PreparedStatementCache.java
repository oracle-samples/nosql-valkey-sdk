/*-
 * Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.valkey.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PreparedStatement;

public class PreparedStatementCache {
	
    private final NoSQLHandle handle;
    private final Map<String,PreparedStatement> refMap;
    private final Map<String,PreparedStatement> valMap;

    public PreparedStatementCache(NoSQLHandle handle)
    {
        this.handle = handle;
        refMap = Collections.synchronizedMap(new IdentityHashMap<>());
        valMap = Collections.synchronizedMap(new HashMap<>());
    }

    private PreparedStatement get(String stmt, boolean byRef) {
        Map<String,PreparedStatement> map = byRef ? refMap : valMap;
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

    public PreparedStatement getByRef(String stmt) {
        return get(stmt, true);
    }

    public PreparedStatement getByVal(String stmt) {
        return get(stmt, false);
    }

}
