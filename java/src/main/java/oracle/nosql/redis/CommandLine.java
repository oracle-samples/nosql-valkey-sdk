/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;

class CommandLine {
    private static final String ARG_REGION = "-region";
    private static final String ARG_ENDPOINT = "-endpoint";
    private static final String ARG_COMPARTMENT = "-compartment";
    private static final String ARG_NAMESPACE = "-namespace";
    private static final String ARG_AUTH = "-auth";
    private static final String ARG_AUTH_FILE = "-auth-file";
    private static final String ARG_AUTH_PROFILE = "-auth-profile";
    private static final String ARG_DELEGATION_TOKEN_FILE =
        "-delegation-token-file";
    private static final String ARG_SERVICE_ACCT_TOKEN_FILE =
        "-service-acct-token-file";
    private static final String ARG_SERVER_CA_CERT_FILE =
        "-server-ca-cert-file";

    private static final String ARG_TABLE_LIMITS = "-table-limits";
    private static final String ARG_HOST = "-host";
    private static final String ARG_PORT = "-port";
    private static final String ARG_MAX_RETRIES = "-max-retries";
    private static final String ARG_CLEANUP_ON_STARTUP =
        "-cleanup-on-startup";

    private static final String AUTH_USER = "user";
    private static final String AUTH_SESS_TOKEN = "session-token";
    private static final String AUTH_INSTANCE = "instance";
    private static final String AUTH_RESOURCE = "resource";
    private static final String AUTH_OKE = "oke";
    private static final String AUTH_KVSTORE = "kvstore";
    private static final String AUTH_CLOUDSIM = "cloudsim";

    private enum AuthType {
        USER,
        SESS_TOKEN,
        INSTANCE,
        RESOURCE,
        OKE,
        KVSTORE,
        CLOUDSIM
    }

    private static class CloudSimProvider implements AuthorizationProvider {

        private static final String id = "Bearer exampleId";
        private static AuthorizationProvider provider =
            new CloudSimProvider();

        private static AuthorizationProvider getProvider() {
            return provider;
        }

        /**
         * Disallow external construction. This is a singleton.
         */
        private CloudSimProvider() {}

        @Override
        public String getAuthorizationString(Request request) {
            return id;
        }

        @Override
        public void close() {}
    }

    private String endpoint;
    private Region region;
    private String compartment;
    private String namespace;
    private AuthType authType;
    private String authFile;
    private String authProfile;
    private String delegationTokenFile;
    private String serviceAcctTokenFile;
    private String serverCaCertFile;
    private TableLimits tableLimits = RedisServerConfig.DEFAULT_TABLE_LIMITS;
    private String host;
    private int port = -1;
    private int maxRetries = RedisServerConfig.DEFAULT_MAX_ATOMIC_RETRIES;
    private boolean cleanupOnStartup =
        RedisServerConfig.DEFAULT_CLEANUP_ELEMS_TABLES_ON_STARTUP;

    public CommandLine(String [] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case ARG_ENDPOINT:
                    endpoint = args[++i];
                    break;
                case ARG_REGION:
                    region = parseRegion(args[++i]);
                    break;
                case ARG_COMPARTMENT:
                    compartment = args[++i];
                    break;
                case ARG_NAMESPACE:
                    namespace = args[++i];
                    break;
                case ARG_AUTH:
                    authType = parseAuthType(args[++i]);
                    break;
                case ARG_AUTH_FILE:
                    authFile = args[++i];
                    break;
                case ARG_AUTH_PROFILE:
                    authProfile = args[++i];
                    break;
                case ARG_DELEGATION_TOKEN_FILE:
                    delegationTokenFile = args[++i];
                    break;
                case ARG_SERVICE_ACCT_TOKEN_FILE:
                    serviceAcctTokenFile = args[++i];
                    break;
                case ARG_SERVER_CA_CERT_FILE:
                    serverCaCertFile = args[++i];
                    break;
                case ARG_TABLE_LIMITS:
                    tableLimits = parseTableLimits(args[++i]);
                    break;
                case ARG_HOST:
                    host = args[++i];
                    break;
                case ARG_PORT:
                    port = chkParsePosInt(args[++i], ARG_PORT);
                    break;
                case ARG_MAX_RETRIES:
                    maxRetries = chkParsePosInt(args[++i], ARG_MAX_RETRIES);
                    break;
                case ARG_CLEANUP_ON_STARTUP:
                    cleanupOnStartup = Boolean.parseBoolean(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Unknown argument " + args[i]);
            }
        }

        if (authType == null) {
            authType = endpoint != null ? AuthType.CLOUDSIM : AuthType.USER;
        }

        final boolean inContainer = RedisServerConfig.isRunningInContainer();
        if (inContainer && (host != null || port != -1)) {
            throw new IllegalArgumentException(
                "Cannot change host or port when running in container");
        }

        if (host == null) {
            host = inContainer ?
                RedisServerConfig.DEFAULT_HOST_IN_CONTAINER :
                RedisServerConfig.DEFAULT_HOST;
        }

        if (port == -1) {
            port = RedisServerConfig.DEFAULT_PORT;
        }

        validate();
    }

    private static int chkParsePosInt(String val, String name) {
        assert val != null;
        assert name != null && !name.isEmpty();

        int res = 0;
        try {
            res = Integer.parseInt(val);
            if (res <= 0) {
                throw new IllegalArgumentException(
                    name + " value must be positive");
            }
            return res;
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                "Invalid " + name + " value: " + val);
        }
    }

    private static Region parseRegion(String val) {
        Region res = Region.fromRegionId(val);
        if (res == null) {
            throw new IllegalArgumentException("Invalid region: " + val);
        }
        return res;
    }

    private static AuthType parseAuthType(String val) {
        switch (val) {
            case AUTH_USER:
                return AuthType.USER;
            case AUTH_SESS_TOKEN:
                return AuthType.SESS_TOKEN;
            case AUTH_INSTANCE:
                return AuthType.INSTANCE;
            case AUTH_RESOURCE:
                return AuthType.RESOURCE;
            case AUTH_OKE:
                return AuthType.OKE;
            case AUTH_KVSTORE:
                return AuthType.KVSTORE;
            case AUTH_CLOUDSIM:
                return AuthType.CLOUDSIM;
            default:
                throw new IllegalArgumentException(
                    "Invalid auth type: " + val);
        }
    }

    private static StoreAccessTokenProvider getSATPFromFile(String filePath) {
        Properties prop = new Properties();
        try(InputStream is = new FileInputStream(filePath)) {
            prop.load(is);
        } catch (IOException ex) {
            throw new IllegalArgumentException(
                "Failed to read kvstore credentials from file", ex);
        }

        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        if (username == null || password == null) {
            throw new IllegalArgumentException(
                "Missing kvstore credentials in file");
        }

        return new StoreAccessTokenProvider(username, password.toCharArray());
    }

    // Must be in the form <read-units>,<write-units>,<storageGB> for
    // provisioned capacity (e.g. 100,100,5) or <storageGB> for on-demand
    // capacity (e.g. 5). Can use "," or ";" as delimiters.
    private static TableLimits parseTableLimits(String val) {
        String[] limits = val.split("[,;]");
        switch (limits.length) {
            case 3:
                return new TableLimits(
                    chkParsePosInt(limits[0], "read units"),
                    chkParsePosInt(limits[1], "write units"),
                    chkParsePosInt(limits[2], "storage GB"));
            case 1:
                return new TableLimits(
                    chkParsePosInt(limits[2], "storage GB"));
            default:
                throw new IllegalArgumentException(
                    "Invalid table limits: " + val);
        }
    }

    private void validate() {
        boolean isCloud = (authType != AuthType.KVSTORE &&
            authType != AuthType.CLOUDSIM);
        if (region != null) {
            if (endpoint != null) {
                throw new IllegalArgumentException(
                    "Cannot specify both endpoint and region");
            }
            if (!isCloud) {
                throw new IllegalArgumentException(
                    "Region may only be specified for Cloud Service");
            }
        }
        if (compartment != null && !isCloud) {
            throw new IllegalArgumentException(
                "Compartment may only be specified for Cloud Service");
        }
        if (namespace != null && authType != AuthType.KVSTORE) {
            throw new IllegalArgumentException(
                "Namespace may only be specified for on-prem service");
        }
        if (authFile != null && (authType != AuthType.USER &&
            authType != AuthType.KVSTORE)) {
            throw new IllegalArgumentException(String.format(
                "%s may be specified only for %s or %s", ARG_AUTH_FILE,
                AUTH_USER, AUTH_KVSTORE));
        }
        if (authProfile != null && authType != AuthType.USER) {
            throw new IllegalArgumentException(String.format(
                "%s may be specified only for %s", ARG_AUTH_PROFILE,
                AUTH_USER));
        }
        if (delegationTokenFile != null && authType != AuthType.INSTANCE) {
            throw new IllegalArgumentException(ARG_DELEGATION_TOKEN_FILE +
                " may only be specified for instance principal");
        }
        if (serviceAcctTokenFile != null && authType != AuthType.OKE) {
            throw new IllegalArgumentException(ARG_SERVICE_ACCT_TOKEN_FILE +
                " may only be specified for OKE");
        }
        if (serverCaCertFile != null && authType != AuthType.KVSTORE) {
            throw new IllegalArgumentException(ARG_SERVER_CA_CERT_FILE +
                " may only be specified for on-prem service");
        }
    }

    private AuthorizationProvider getAuthProvider() {
        switch (authType) {
            case USER:
                try {
                    return authFile != null ?
                        (authProfile != null ?
                            new SignatureProvider(authFile, authProfile) :
                            new SignatureProvider(authFile, "DEFAULT")) :
                        (authProfile != null ?
                            new SignatureProvider(authProfile) :
                            new SignatureProvider());
                } catch (IOException ex) {
                    throw new IllegalArgumentException(
                        "Error loading profile from OCI config file", ex);
                }
            case INSTANCE:
                return delegationTokenFile != null ?
                    SignatureProvider
                        .createWithInstancePrincipalForDelegation(
                            new File(delegationTokenFile)) :
                    SignatureProvider.createWithInstancePrincipal();
            case RESOURCE:
                return SignatureProvider.createWithResourcePrincipal();
            case OKE:
                return serviceAcctTokenFile != null ?
                    SignatureProvider.createWithOkeWorkloadIdentity(
                        new File(serviceAcctTokenFile), null) :
                    SignatureProvider.createWithOkeWorkloadIdentity();
            case KVSTORE:
                return authFile != null ?
                    getSATPFromFile(authFile) : new StoreAccessTokenProvider();
            case CLOUDSIM:
                return CloudSimProvider.getProvider();
            default:
                assert false;
                return null;
        }
    }

    private NoSQLHandleConfig getNoSQLConfig() {
        AuthorizationProvider authProvider = getAuthProvider();
        NoSQLHandleConfig cfg;
        if (region != null) {
            cfg = new NoSQLHandleConfig(region, authProvider);
        } else if (endpoint != null) {
            cfg = new NoSQLHandleConfig(endpoint, authProvider);
        } else {
            // If neither region nor endpoint is specified then, if using
            // Cloud Service, the region will be retrieved by the auth
            // provider; if using on-prem or Cloudsim, we default to endpoint
            // "localhost:8080".
            cfg = authType == AuthType.USER ?
                new NoSQLHandleConfig(authProvider) :
                new NoSQLHandleConfig(RedisServerConfig.DEFAULT_NOSQL_ENDPOINT,
                    authProvider);
        }

        if (compartment != null) {
            cfg.setDefaultCompartment(compartment);
        } else if (namespace != null) {
            cfg.setDefaultNamespace(namespace);
        }

        return cfg;
    }

    public static String usage() {
        final String startArg = "\n\t [ ";
        final String endArg = " ]";
        final String orSep = " | ";

        StringBuilder sb = new StringBuilder();

        sb.append("Usage: java oracle.nosql.redis.NoSQLRedisServer ");
        sb.append(startArg).append(ARG_REGION).append(" <region>")
            .append(orSep).append(ARG_ENDPOINT).append(" <endpoint>")
            .append(endArg);
        sb.append(startArg).append(ARG_COMPARTMENT).append(" <compartment>")
            .append(endArg);
        sb.append(startArg).append(ARG_NAMESPACE).append(" <namespace>")
            .append(endArg);
        sb.append(startArg).append(ARG_AUTH)
            .append(' ').append(AUTH_USER)
            .append(orSep).append(AUTH_SESS_TOKEN)
            .append(orSep).append(AUTH_INSTANCE)
            .append(orSep).append(AUTH_RESOURCE)
            .append(orSep).append(AUTH_OKE)
            .append(orSep).append(AUTH_KVSTORE)
            .append(orSep).append(AUTH_CLOUDSIM)
            .append(endArg);
        sb.append(startArg).append(ARG_AUTH_FILE).append(" <auth-file>")
            .append(endArg);
        sb.append(startArg).append(ARG_AUTH_PROFILE).append(" <auth-profile>")
            .append(endArg);
        sb.append(startArg).append(ARG_DELEGATION_TOKEN_FILE)
            .append(" <delegation-token-file>").append(endArg);
        sb.append(startArg).append(ARG_SERVICE_ACCT_TOKEN_FILE)
            .append(" <service-acct-token-file>").append(endArg);
        sb.append(startArg).append(ARG_SERVER_CA_CERT_FILE)
            .append(" <server-ca-cert-file>").append(endArg);
        sb.append(startArg).append(ARG_HOST).append(" <host>").append(endArg);
        sb.append(startArg).append(ARG_PORT).append(" <port>").append(endArg);
        sb.append(startArg).append(ARG_MAX_RETRIES).append(" <max-retries>")
            .append(endArg);
        sb.append(startArg).append(ARG_CLEANUP_ON_STARTUP)
            .append(" <cleanup-on-startup>").append(endArg);

        return sb.toString();
    }

    public RedisServerConfig getRedisServerConfig() {
        return new RedisServerConfig(getNoSQLConfig(), tableLimits, host, port,
            maxRetries, cleanupOnStartup);
    }

}
