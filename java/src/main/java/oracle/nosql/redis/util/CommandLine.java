/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.redis.util;

import java.io.IOException;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.Request;

/*
 * Common is a companion class to the Oracle NoSQL Cloud Service examples. It
 * provides a few utilities for parsing and validating the arguments used by
 * the examples. It also include a method to loop on query results until they
 * are all consumed. This is important when a query returns more results than
 * can fit in a single response.
 *
 * Examples can be run against:
 *  1. the cloud service
 *  2. the cloud simulator (CloudSim)
 *  3. the on-premises proxy and Oracle NoSQL Database instance, secure or
 *  not secure.
 *
 * If running against the cloud service, the arguments are:
 *   java Example <region> [-configFile <path-to-config-file]
 *
 * The Region is one of the Region constants from Region.java and that
 * Region's endpoint will be used for the connection.
 *
 * If the configFile file path is not provided the uses the default
 * constructor for SignatureProvider, which looks for the file
 * $HOME/.oci/config
 *
 * For example:
 *   java -cp .:../lib/nosqldriver.jar BasicTableExample us-ashburn-1
 *
 *
 * If running against CloudSim:
 *   java Example <endpoint>
 *
 * For example:
 *
 *   java -cp .:../lib/nosqldriver.jar BasicTableExample \
 *                http://localhost:8080
 *
 * If running against the on-premises Oracle NoSQL Database:
 *   java Example <endpoint> -useKVProxy [-user <user>] [-password password]
 *
 * User and password are required when accessing a secure store.
 *
 * For example, run against a not-secure proxy and store, with the proxy
 * running on port 80:
 *   java -cp .:../lib/nosqldriver.jar BasicTableExample \
 *                http://localhost:80 -useKVProxy
 *
 * Run against a secure proxy and store, with the proxy running on port 443,
 * with:
 *  1. a driver.trust file in the current directory with the password "123456"
 *  2. user "driver" with password "Driver.User@01". This user must have
 *  been created in the store and must have permission to create and use tables:
 *    java -Djavax.net.ssl.trustStorePassword=123456                    \
 *      -Djavax.net.ssl.trustStore=driver.trust -cp .:../lib/nosqldriver.jar \
 *     BasicTableExample https://localhost:443 -useKVProxy -user driver \
 *     -password Driver.User@01
 *
 * Credential Setup
 * ----------------
 * If you are running against the cloud service, you will need to
 * provide configFile. This step is not needed if you are running against
 * the Cloud Simulator or the on-premises proxy.
 *
 * By default, with no -configFile argument, the example code will use
 * the default SignatureProvider constructor which uses the default user
 * profile and private key from default path of configuration
 * file $HOME/.oci/config
 *
 * There are additional SignatureProvider constructors that can be used
 * if a configuration file is not available or desirable. These allow
 * an application to directly supply required configuration information.
 *
 * It is possible to specify a different configuration file using the
 * -configFile flag. In this case the default user profile is still used.
 */
public class CommandLine {

    private static final String PROXY_FLAG = "-useKVProxy";
    private static final String USER_FLAG = "-user";
    private static final String PASSWORD_FLAG = "-password";
    private static final String CONFIG_FLAG = "-configFile";

    private String endpoint;
    private boolean useCloudService;
    private boolean useCloudSim;
    private boolean useKVProxy;
    private String user;
    private char[] password;
    private String configFile;

    public CommandLine(String [] args) {
        validate(args);
    }

    /**
     * Parse and validate the endpoint, check that configFile were filled in
     * @return true if the endpoint is for the cloud service, false if it is
     * for CloudSim.
     */
    private void validate(String [] args) {

        if (args.length < 1) {
            usage(null);
        }

        endpoint = args[0];

        /*
         * Is the argument an endpoint or OCI Region? If so, acquire the
         * String endpoint for the region. The NoSQLHandleConfig constructor
         * can handle the region id as an endpoint. This code helps figure
         * out if the example is using the cloud service or not to help
         * validate arguments.
         */
        Region region = Region.fromRegionId(endpoint);
        if (region != null) {
            useCloudService = true;
            endpoint = region.endpoint();
        } else if (endpoint.startsWith("nosql.")) {
            /* a cloud service endpoint */
            useCloudService = true;
        }

        /*
         * Let the system validate a CloudSim or KV endpoint.
         */
        checkArgs(args);
        return;
    }

    /**
     * Check args
     */
    private void checkArgs(String[] args) {

        int currentArg = 1;
        while (currentArg < args.length) {
            String nextArg = args[currentArg++];
            if (PROXY_FLAG.equals(nextArg)) {
                useKVProxy = true;
                if (useCloudService) {
                    usage("PROXY_FLAG cannot be used with " +
                          "cloud service endpoint");
                }
            } else if (USER_FLAG.equals(nextArg)) {
                if (useCloudService) {
                    usage("USER_FLAG cannot be used with " +
                          "cloud service endpoint");
                }
                user = args[currentArg++];
            } else if (PASSWORD_FLAG.equals(nextArg)) {
                if (useCloudService) {
                    usage("PASSWORD_FLAG cannot be used with " +
                          "cloud service endpoint");
                }
                password = args[currentArg++].toCharArray();
            } else if (CONFIG_FLAG.equals(nextArg)) {
                if (!useCloudService) {
                    usage("Cannot use " + CONFIG_FLAG + " with the " +
                          "cloud simulator");
                }
                configFile = args[currentArg++];
            } else {
                usage("Unknown flag: " + nextArg);
            }
        }

        if (!useCloudService) {
            if (configFile != null) {
                usage("Cannot use " + CONFIG_FLAG + " with the proxy");
            }
            if (!useKVProxy) {
                useCloudSim = true;
                if (user != null || password != null) {
                    usage("User and password are not valid " +
                          "with the cloud simulator");
                }
            }
        }
    }

    private void usage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
        System.err.println(
            "Usage: java oracle.nosql.redis.NoSQLRedisServer " +
            "<endpoint> " +
            "\n\t [ " + PROXY_FLAG + "]" +
            "\n\t [ " + CONFIG_FLAG + "]" +
            "\n\t [ " + USER_FLAG + " <userName>]" +
            "\n\t [ " + PASSWORD_FLAG + " <password>]");
        System.exit(1);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean useCloudService() {
        return useCloudService;
    }

    public boolean useCloudSim() {
        return useCloudSim;
    }

    public boolean useKVProxy() {
        return useKVProxy;
    }

    public String getUserName() {
        return user;
    }

    public char[] getPassword() {
        return password;
    }

    /**
     * Return an appropriate AuthorizationProvider:
     *  Cloud Service - SignatureProvider
     *  KV Proxy - StoreAccessTokenProvider
     *  Cloud Simulator - CloudSimProvider
     */
    public AuthorizationProvider getAuthProvider() {
        try {
            if (useCloudService) {
                if (configFile != null) {
                    return new SignatureProvider(configFile, "DEFAULT");
                }
                /* default looks for the file, $HOME/.oci/config */
                return new SignatureProvider();

                /*
                 * If you want to directly provide credentials rather than
                 * use a configuration file use this SignatureProvider
                 * constructor:
                return new SignatureProvider(tenantId, // OCID
                                             userId,   // OCID
                                             fingerprint, // String
                                             File privateKeyFile,
                                             char[] passphrase);
                 */
            } else if (useCloudSim) {
                return CloudSimProvider.getProvider();
            }
            assert(useKVProxy);
            /* if user is not set, assume not secure */
            if (user == null) {
                return new StoreAccessTokenProvider();
            }
            return new StoreAccessTokenProvider(user, password);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(ioe);
        }
    }

    /**
     * A simple provider that uses a manufactured id for use by the
     * Cloud Simulator. It is used as a namespace for tables and not
     * for any actual authorization or authentication.
     */
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
}
