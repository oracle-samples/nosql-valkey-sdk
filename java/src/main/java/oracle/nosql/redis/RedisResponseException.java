/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import oracle.nosql.driver.NoSQLException;
 
 public class RedisResponseException extends Exception {
	 
    public static enum ErrorPrefix {
        ERR,
        WRONGTYPE,
        PROTOCOL,
        CORRUPT,
        NOSQL
    }

    public RedisResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisResponseException(ErrorPrefix errPrefix, String message,
        Throwable cause) {
        this(errPrefix.name() + " " + message, cause);
    }

    public RedisResponseException(ErrorPrefix errPrefix, String message) {
        this(errPrefix, message, null);
    }

    public static RedisResponseException numArgs(String cmdName) {
        return new RedisResponseException(ErrorPrefix.ERR,
            String.format("wrong number of arguments for '%s' command",
                cmdName.toLowerCase()));
    }

    public static RedisResponseException corrupt(String msg) {
        return new RedisResponseException(ErrorPrefix.CORRUPT,
            "Encountered corrupted data format" +
                msg == null ? "" : ": " + msg);
    }

    public static RedisResponseException wrongType() {
        return new RedisResponseException(ErrorPrefix.WRONGTYPE,
            "Operation against a key holding the wrong kind of value");
    }

    public static RedisResponseException syntaxError() {
        return new RedisResponseException(ErrorPrefix.ERR, "syntax error");
    }

    public static RedisResponseException nosql(NoSQLException ex) {
        return new RedisResponseException(ErrorPrefix.NOSQL, ex.toString(),
            ex);
    }

    public static RedisResponseException unknownCommand(RawCommand cmd) {
        // Todo: print "with arguments beginning with" followed by args as
        // done by real Redis server.
        return new RedisResponseException(ErrorPrefix.ERR,
            "Unknown command: " + cmd.name);
    }

}
