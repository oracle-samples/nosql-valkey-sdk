# Oracle NoSQL Valkey/Redis API Adapter
## About

Oracle NoSQL Valkey/Redis API Adapter (API Proxy) provides a way for applications to use Oracle NoSQL
database as a [Redis](https://redis.io/about/) store. It allows you to store
Valkey/Redis types and data structures persistenly in Oracle NoSQL database tables
and access them using any of supported [Valkey Clients](https://valkey.io/clients/),
[Redis Clients](https://redis.io/docs/latest/develop/clients/) or a command
line interface.

The API proxy runs as a TCP listener and uses the
[Redis Serialization Protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/)
(RESP) to interact with clients -- Valkey information located here [Valkey protocol](https://valkey.io/topics/protocol/). 
You can install and run the API proxy as a
Docker container. You can also invoke it on the command line using Java. For
Java developers, there are also APIs to start and run the API proxy within
the application process.

## Prerequisites

1. Container Engine such as Docker, Rancher Desktop, etc. to install as Docker
container

    or

    Java Runtime 11 or later to install as Java archive.

2. [Redis CLI](https://redis.io/docs/latest/develop/tools/cli/) or [Valkey CLI](https://valkey.io/topics/cli/)

    or one of
    [Redis Client API Libraries](https://redis.io/docs/latest/develop/clients/) or  [Valkey Clients API Libraries](https://valkey.io/clients/)


4. Access to Oracle NoSQL Database.

    For use with the Oracle NoSQL Database Cloud Service:
    * An Oracle Cloud Infrastructure account
    * A user created in that account, in a group with a policy that grants the
    desired permissions.
    See
    [Oracle NoSQL Database Cloud Service](https://docs.oracle.com/en/cloud/paas/nosql-cloud/index.html)
    for more information.

    For Cloud Simulator, see
    [Oracle NoSQL Cloud Simulator](https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html).

    For use with the Oracle NoSQL Database On Premise:
    * [Oracle NoSQL Database](https://www.oracle.com/database/technologies/related/nosql.html).

    Note: the API proxy only supports Oracle NoSQL Database Server version
    25.3 and later.

    See
[Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html)
to download Oracle NoSQL Database. See
[Oracle NoSQL Database Documentation](https://docs.oracle.com/en/database/other-databases/nosql-database/index.html)
to get started with Oracle NoSQL Database. In particular, see
[Administrator Guide](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/admin/index.html)
on how to install, configure and run Oracle NoSQL Database Service.

## Installation

You can install the API proxy as either:

* Docker container from the GitHub Container Registry:

  ```bash
     docker pull ghcr.io/oracle/nosql-valkey-api:<tag>
     docker tag ghcr.io/oracle/nosql-valkey-api:<tag> oracle/nosql-valkey-api
  ```

    where <tag> is the tag for the image (use *latest* for the latest build).

* Java archive from Maven Central:

    To run as a standalone Java program, download the jar with dependencies:

    ```bash
    mvn dependency:copy \
      -Dartifact=com.oracle.nosql.valkey:nosql-valkey:<version>:jar:jar-with-dependencies \
      -DoutputDirectory=<download-directory>
    ```

    This should download *nosql-valkey-<version>-jar-with-dependencies.jar*
(where <version> is the version, e.g. 1.0.0). Alternatively, you can download
it manually from [Maven Central](link to the project artifacts).

    To start the API proxy within your Java application, add it as
dependency of your project in *pom.xml*:

    ```xml
    <dependency>
      <groupId>com.oracle.nosql.valkey</groupId>
      <artifactId>nosql-valkey</artifactId>
      <version>0.1.0</version>
    </dependency>
    ```

    Change the version above to the desired version.

## Quickstart

1. Download and install
[Oracle NoSQL Database](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html)
Enterprise Edition.

2. Download and install (Equivalent Valkey components can be used)
[Redis CLI](https://redis.io/docs/latest/develop/tools/cli/) as indicated.

    Alternatively you may install *redis-tools* package (for Debian or Ubuntu
Linux) which includes *redis-cli*:

    ```bash
    sudo apt update
    sudo apt install redis-tools
    ```

3. Install the API proxy as a Docker container as described in
[Installation](#installation) section.

4. Run KVLite. E.g.:

    ```bash
    $ cd <kv-install-dir>/kv-25.3.21/lib
    $ java -jar kvstore.jar kvlite -root <kv-root-dir> -store kvstore \
      -secure-config disable
    Created new kvlite store with args:
    -root /home/ypolonsk/test/kv/kvstore-ns/ -store kvstore \
      -host <hostname> -port 5000 -admin-web-port -1 -secure-config disable
    ```

5. Run NoSQL Database Proxy. E.g.:

    ```bash
    $ cd <kv-install-dir>/kv-25.3.21/lib
    $ java -jar httpproxy.jar -httpPort 8080 -storeName kvstore \
      -helperHosts localhost:5000 -verbose true
    Starting HTTP Proxy
    Proxy started:
    ...
    ```

6. On another terminal, run API proxy:
(Note that NoSQL endpoint defaults to *host.docker.internal:8080*, see
[Command Line Parameters](#command-line-parameters)).

    ```bash
    $ docker run --rm -p 6379:6379 oracle/nosql-valkey-api -auth kvstore
    Nov 25, 2025 3:25:55 AM io.netty.handler.logging.LoggingHandler channelRegistered
    INFO: [id: 0xae728060] REGISTERED
    Nov 25, 2025 3:25:55 AM io.netty.handler.logging.LoggingHandler bind
    INFO: [id: 0xae728060] BIND: /0.0.0.0:6379
    Nov 25, 2025 3:25:55 AM io.netty.handler.logging.LoggingHandler channelActive
    INFO: [id: 0xae728060, L:/[0:0:0:0:0:0:0:0]:6379] ACTIVE
    ```

6. On another terminal start *redis-cli* or *valkey-cli*. No arguments are necessary since
it uses the same default host and port. Then execute some commands. For
example:

    ```bash
    $ valkey-cli
    127.0.0.1:6379> set key1 value1
    OK
    127.0.0.1:6379> expire key1 1000
    (integer) 1
    127.0.0.1:6379> ttl key1
    (integer) 986
    127.0.0.1:6379> copy key1 key2
    (integer) 1
    127.0.0.1:6379> get key1
    "value1"
    127.0.0.1:6379> append key2 2
    (integer) 7
    127.0.0.1:6379> get key2
    "value12"
    127.0.0.1:6379> keys key*
    1) "key1"
    2) "key2"
    127.0.0.1:6379> lpush l1 5 4 3 2 1
    (integer) 5
    127.0.0.1:6379> lrange l1 0 -1
    1) "1"
    2) "2"
    3) "3"
    4) "4"
    5) "5"
    127.0.0.1:6379> rpop l1
    "5"
    127.0.0.1:6379> llen l1
    (integer) 4
    127.0.0.1:6379> hset h1 field1 value1 field2 value2 field3 1
    (integer) 3
    127.0.0.1:6379> hincrby h1 field3 2
    (integer) 3
    127.0.0.1:6379> hgetall h1
    1) "field1"
    2) "value1"
    3) "field2"
    4) "value2"
    5) "field3"
    6) "3"
    127.0.0.1:6379> json.set json1 $ '{ "a": "val", "b": { "x": 1 }, "c": { "y": 1, "arr": [] }}'
    OK
    127.0.0.1:6379> json.set json1 $.d '{ "arr2": [] }'
    OK
    127.0.0.1:6379> json.get json1
    "{\"a\":\"val\",\"b\":{\"x\":1},\"c\":{\"arr\":[],\"y\":1},\"d\":{\"arr2\":[]}}"
    127.0.0.1:6379> json.numincrby json1 $.*.* 2
    "[3,null,3,null]"
    127.0.0.1:6379> json.arrappend json1 $.*.* 10
    1) (nil)
    2) (integer) 1
    3) (nil)
    4) (integer) 1
    127.0.0.1:6379> json.get json1
    "{\"a\":\"val\",\"b\":{\"x\":3},\"c\":{\"arr\":[10],\"y\":3},\"d\":{\"arr2\":[10]}}"
    127.0.0.1:6379> scan 0 count 3
    1) "210271729058178054"
    2) 1) "json1"
      2) "h1"
      3) "l1"
    127.0.0.1:6379> scan 210271729058178054 count 3
    1) "0"
    2) 1) "key1"
      2) "key2"
    ```

## Running Oracle NoSQL API Proxy

### NoSQL Database Environments

You can use the API proxy to store and access data using one of these
Oracle NoSQL Database services:

* Oracle NoSQL Database Cloud Service
* On-Premise Oracle NoSQL Database
* Oracle NoSQL Database Cloud Simulator

The API proxy supports the same connection and authentication parameters as
supported by
[Oracle NoSQL Database language SDKs](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/nsdev/oracle-nosql-database-sdk-drivers.html),
e.g. [Oracle NoSQL Java SDK](https://github.com/oracle/nosql-java-sdk).

You can connect the API proxy to Oracle NoSQL Cloud Service using User
Credentials, session token, instance principal, resource principal or OKE
workload identity.
The API proxy can also connect to On-Premise Oracle NoSQL Database (using
secure or non-secure mode) and Oracle NoSQL Database Cloud Simulator.

The connection configuration is specified when starting the API proxy and
will be described in the [Command Line Parameters](#command-line-parameters)
section. The API proxy will use a group of tables (described in
[Schema and Data Format](#nosql-database-schema-and-data-format) section) to
store Redis/Valkey data. To avoid naming conflicts it is recommended to use separate
[compartment](https://docs.oracle.com/en/cloud/foundation/cloud_architecture/governance/compartments.html)
when connecting to Oracle NoSQL Cloud Service or
[namespace](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/sqlreferencefornosql/namespace-management.html)
when connection to On-Premise Oracle NoSQL Database. These can be specified as
configuration parameters when starting the API proxy.

### Run As Docker container

```bash
docker run [-d] [--rm] -p [<api_proxy_host>:]<api_proxy_port>:6379 \
  [-v host_path1:container_path1 -v host_path2:container_path2 ...] \
  oracle/nosql-valkey-api [-param1 value1 -param2 value2 ...]
```

In the above:

* Use *-d* to optionally run container in the background.
* Use *--rm* to optionally remove container when it exits.
* Use *-p* to map container port 6379 to your chosen hostname (or ip address)
and the port on the host. The API proxy listens on port 6379 in the
container. This port needs to be mapped to the port on your host in order for
Redis/Valkey clients to connect to it.
* Use *-v* to optionally map files and directories from the host to the
container. Note that any file paths you use as part of command line parameters
or configuration files are the paths inside the container. You need to map
corresponding paths from the host to the container in order for the API
proxy to find them.
* The docker image name is optionally followed by command line parameters for
the API proxy, which are described in
[Command Line Parameters](#command-line-parameters) section.

For example:

```bash
docker run -rm -p 6379:6379 -v ~/.oci/config:~/.oci/config \
  -v ~/oracle/api-proxy/oci_api_key.pem:~/oracle/api-proxy/oci_api_key.pem \
  nosql-valkey-api -auth user -region us-phoenix-1 -compartment users/john
```

In the above example, we are connecting the API proxy to Oracle NoSQL
Database Cloud Service in region *us-phoenix-1* using user's credentials. By
default, the credentials are stored in OCI config file *~/.oci/config*
(where ~ is user's home directory), hence the mapping of this file from the
host to the container. This config file may contain lines such as:

```ini
[DEFAULT]
user=ocid1.user.oc1..<user_id>
fingerprint=<fingerprint>
key_file=~/oracle/api-proxy/oci_api_key.pem
tenancy=ocid1.tenancy.oc1..<tenant_id>
```

In order to connect, we also need user's private key file, located at
*~/oracle/api-proxy/oci_api_key.pem* in this example, hence we also have to
map this file from the host to the container at the same location (since we
are using credentials from the host's OCI config file).

This can be simplified if we store private key file in the same diretory, in
which case we can just map the directory. In addition, we can also put the
region in config file so that it doesn't have to be passed as a parameter:

```ini
[DEFAULT]
user=ocid1.user.oc1..<user_id>
fingerprint=<fingerprint>
key_file=~/.oci/oci_api_key.pem
tenancy=ocid1.tenancy.oc1..<tenant_id>
region=us-phoenix-1
```

```bash
docker run -rm -p 6379:6379 -v ~/.oci:~/.oci nosql-valkey-api -auth user \
  -compartment users/john
```

Note: when the API proxy is running in container and you use *endpoint*
parameter (see [Command Line Parameters](#command-line-parameters) section) to
connect to Oracle NoSQL Database running on your localhost (e.g. Cloud
Simulator or local KVLite), the hostname within the endpoint should not be
*localhost* because this points to the localhost of the container itself.
Instead, use *host.docker.internal* as the hostname,
e.g. *host.docker.internal:8080*.

### Run As Java Program

The API proxy requires minimum Java 11. You can run the API proxy as
follows:

```bash
java -cp path/to/nosql-valkey-<version>-jar-with-dependencies.jar \
  oracle.nosql.valkey.NoSQLRedisServer [-param1 value1 -param2 value2 ...]
```

The main class name (oracle.nosql.valkey.NoSQLRedisServer) is optionally
followed by command line parameters for the API proxy, which are described
in [Command Line Parameters](#command-line-parameters) section.

From the last example in the previous section:

```bash
java -cp path/to/nosql-valkey-<version>-jar-with-dependencies.jar \
  oracle.nosql.valkey.NoSQLRedisServer -auth user -compartment users/john
```

### Run within Java Application

You may also start the API proxy programmatically within your Java
application. The following configuration is equivalent to the previous
example:

```java
  import oracle.nosql.driver.NoSQLHandleConfig;
  import oracle.nosql.driver.iam.SignatureProvider;
  ...
  NoSQLHandleConfig nosqlConfig = new NoSQLHandleConfig(
    new SignatureProvider());
  nosqlConfig.setDefaultCompartment("users/john");
  NoSQLRedisServer redisSvr = new NoSQLRedisServer(
    new RedisServerConfig(nosqlConfig));
  redisSvr.start();
  ...
  redisSvr.stop(5000);
```

Note that it is advised to stop the API proxy (see *redisSvr.stop*
above) before you exit your application.

For more information, see [NoSQL API proxy Javadoc](link needed) as well as
[Javadoc for Oracle NoSQL Java SDK](https://oracle.github.io/nosql-java-sdk/).

### Command Line Parameters

The parameters are used to specify Oracle NoSQL Database environment to which
the API proxy would connect, as well as some API proxy-specific
configuration settings.

The connection and authentication parameters are based on connection
parameters and authentication types used by SDKs for both Oracle NoSQL
Database Cloud Service and On-Premise Oracle NoSQL Database.

For more details, see the following:

* For Cloud Service, see
[Connect to Oracle NoSQL Database Cloud Service](https://docs.oracle.com/en/cloud/paas/nosql-cloud/tasks_connect.html)
* For On-Prem Database, see
[Oracle NoSQL Database Proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/admin/proxy.html)
* For Cloud Simulator, see
[Developing in Oracle NoSQL Database Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/donsq/index.html)

The API proxy accepts the following parameters:

* -region <region> (Cloud only) Region to use to connect to Oracle NoSQL
Database Cloud Service. If not specified, the region will be inferred from
OCI config file if any.

* -endpoint <endpoint> Endpoint to connect to Oracle NoSQL Database. Cannot be
used together with *-region*. If not specified, and running with Cloud
Service, it is assumed the endpoint will be inferred from the OCI config file
(otherwise an error is returned). If running with On-Prem Database or Cloud
Simulator, the endpoint defaults to: *host.docker.internal:8080* if running
inside the container, or *localhost:8080* otherwise.

* -compartment <compartment> (Cloud only) Compartment to use with Oracle NoSQL
Database Cloud Service. If not specified, defaults to root compartment of the
tenancy.

* -namespace <namespace> (On-prem only) Namespace to use with Oracle NoSQL
Database. If not specified, root (global) namespace is used.

* -auth <auth-type> Authentication type. Must be one of these values:

  *user* - (Cloud only) Authenticate with user credentials. The credentials
  must be present in OCI configuration file. See *-auth-file* and
  *-auth-profile* parameters.
  
  *session-token* - (Cloud only) Authenticate with session token. The
  credentials and the auth token file must be present in OCI configuration
  file. See *-auth-file* and *-auth-profile* parameters.
  
  *instance* - (Cloud only) Authenticate with instance principal.

  *resource* - (Cloud only) Authenticate with resource principal.
  
  *oke* - (Cloud only) Authenticate with Container Engine for Kubernetes (OKE)
  workload identity.
  
  *kvstore* - (On-prem only) Authenticate with on-prem KVStore. See
  *-auth-file* parameter.

  *cloudsim* - Authenticate with the Cloud Simulator.

If not specified, the default is as follows: if endpoint is specified,
defaults to *cloudsim*, otherwise defaults to *user*.

* -auth-file If authentication type is *user* or *session-token*, this
specifies the path to the OCI config file. If authentication type is
*kvstore*, specifies the path to the on-prem auth file containing user
credentials, in which case it is assumed API proxy is connecting to
secure on-prem KVStore. On-prem auth file must be in the following format:

    ```ini
    username=<username>
    password=<password>
    ```

For any other auth type, specifying this parameter is an error.

If not specified, the default is as follows: if auth type is *user* or
*session-token*, the default path to OCI config file is *~/.oci/config*, where
*~* is user's OS home directory. If auth type is *kvstore* and *-auth-file* is
not specified, it is assumed API proxy is connecting to non-secure KVStore.

* -auth-profile (Cloud only) If auth type is *user* or *session-token*,
specifies the profile within the OCI congfile that is used to store user's
credentials. If not specified, the default is *DEFAULT*. For any other auth
type, specifying this parameter is an error.

* -delegation-token-file (Cloud only) If auth type is *instance*, specifies
the path to the file that stores delegation token. For any other auth type,
specifying this parameter is an error.

* -service-acct-token-file (Cloud only) If auth type is *oke*, specifies the
path to the file that stores service account token. If not specified, default
service account token file will be used. For any other auth type, specifying
this parameter is an error.

* -server-ca-cert-file (On-prem only) If auth type is *kvstore* and the
connection is to secure KVStore (thus *-auth-file* is also specified),
specifies the path to the file containing X509 certificate for the server's
certificate authority (CA). This is needed only if the server's certificate is
signed with non-public CA. The CA certificate must be in PEM format. For any
other auth type, specifying this parameter is an error.

* -table-limits (Cloud only) Specifies table limits for the table used to
store Redis/Valkey data. See
[Schema and Data Format](#nosql-database-schema-and-data-format) section for
information on the database schema used. Table limits must be in the format
_\<read-units\>,\<write-units\>,\<storageGB_\> if using provisional capacity,
or just *<storageGB>* for on-demand capacity.

    E.g.:

    For provional capacity: *100,100,5*

    For on-demand capacity: *5*

    If not specified, the default limits are as follows: 100 read units, 100 write
units, 5 GB of storage.

    Note that this parameter only has effect when starting the API proxy for the
first time when the database schema is created, otherwise it is ignored.

* -host Host on which API proxy will listen for connections. If not
specified, defaults to *localhost*. This parameter is not valid if running
the API proxy as Docker container. In this case, use port mapping to map to
your chosen hostname/ip on the host.

* -port Port on which API proxy will listen for connections. If not
specified, defaults to *6379*. This parameter is not valid if running
the API proxy as Docker container. In this case, use port mapping to map to
your port on the host.

* -max-retries The limit on the number of retries of certain operations if
version mismatch is detected due to concurrent operation on the same key.
See [Concurrency Control](#concurrency-control) section. The default is *100*.
After the limit is reached, an error will be returned to the application.

* -cleanup-on-startup Whether, on API proxy startup, to run a background
cleanup thread that will check for and purge any abandoned collection element
data that was left due to previous abnormal termination. The values are
*true*/*false*. The default is *true*. For more information, see restriction 3
in [Generic Commands](#generic-commands) section.

Examples (based on running the API proxy as a Docker container):

1. Connect to Cloud Service with Instance principal, change max-retries value:

    ```bash
    docker run -rm -p 6379:6379 oracle/nosql-valkey-api -auth instance \
      -region us-phoenix-1 -compartment users/john -max-retries 20
    ```

2. Connect to On-prem database on running on localhost, non-secure:

    ```bash
    docker run -rm -p 6379:6379 oracle/nosql-valkey-api \
      -endpoint http://localhost:8080 -auth kvstore
    ```

3. Connect to On-prem database on running on localhost, secure:

    ```bash
    docker run -rm -p 6379:6379 -v ~/valkey_api/kvauth:~/kvauth \
      nosql-valkey-api -endpoint https://localhost:8081 -auth kvstore \
      -auth-file ~/kvauth
    ```

4. Connect to Cloud Simulator running on localhost:

    ```bash
    docker run -rm -p 6379:6379 nosql-valkey-api -endpoint http://localhost:8080 \
      -auth cloudsim
    ```

## Supported Features and Limitations

### Concurrency

Each running proxy can serve many Redis/Valkey clients. Any client that uses TCP and
speaks the
[RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) protocol
may connect and use the API proxy. The current protocol supported is RESP2.

In addtion, multiple proxies can be run that connect to the same Oracle NoSQL
Service and destination (including region/endpoint and compartment/namespace)
and thus will share the same keyspace and data.

Multiple Redis/Valkey clients connected to the same proxy or different proxies may
issue concurrent reads and updates of data stored by the same key,
including creation or deletion of a key. E.g. multiple clients may be updating
the same string, or inserting and deleting elements from the same list.

The API proxy will guarantee the data consistency and atomicity of each
Redis/Valkey command during concurrent updates, although certain limitations may
apply to the atomicity of some commands:

* Multikey update commands such as
[MSET](https://redis.io/docs/latest/commands/mset/) or
[DEL](https://redis.io/docs/latest/commands/del/) or Valkey equivalents when used with multiple
keys, have limitation that the command can be performed atomically with at
most 50 keys. E.g. MSET disallows update of more than 50 keys and DEL, when
provided with more than 50 keys, will split them in groups of 50 or less and
sequentially perform atomic delete on each group and as such will not be
overall atomic.
* Commands on collections that may take multiple elements, such as
[LPUSH](https://redis.io/docs/latest/commands/lpush/),
[LPOP](https://redis.io/docs/latest/commands/lpop/),
[HSET](https://redis.io/docs/latest/commands/hset/), Valkey equivalents, etc. will not be atomic
if provided more than 49 elements for each command, but instead the elements
will be split into groups of 49 or less and atomic operation performed on each
group.

In the cases above when an update command is split into smaller parts, the
overall operation may no longer be atomic, because other concurrent operations
may interleave. In addition, in case of system failure it is possible that
only part of the overall command becomes durable. However, the data
consistency is still guaranteed because each of the smaller parts is
self-contained.

#### Concurrency Control

The API proxy uses version-based concurrency control. In highly concurrent
environment, this means that an operation may have to be retried due to
version mismatch caused by a concurrent transaction. The limit to the number
of retries for each command defaults to 100. You can also change this limit by
using *-max-retries* parameter when starting the API proxy. After the
number of reties reaches the limit, an error will be returned to the
application.

### Data Partitioning

Oracle NoSQL API proxy implements data distribution and scaling as specified
in
[Valkey Cluster](https://valkey.io/topics/cluster-spec/).
The data scales horizontally by being distributed accross multiple shards.

The key space is split into 16384 hash slots. The data for keys belonging to
the same slot is guaranteed to be stored on the same shard of Oracle NoSQL
Database. Effectively, this means that the slot number serves as a shard key
for particular Redis/Valkey key. The slots are computed in similar mannner to Redis/Valkey
Cluster by using *CRC16(key) mod 16384*.

Just as in Redis/Valkey Cluster, any command that can operate on multiple keys, such
as MSET, MGET, DEL, etc. is only allowed when all the keys passed to it belong
to the same slot. This corresponds to Oracle NoSQL Database requirement that
only allows atomic operations on data that belongs to the same shard.

Also, as in Redis/Valkey Cluster, to facilitate creation of such keys, you can use
[Hash Tags](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags).
So, for example, the keys such as *name:{user12345}* and *address:{user12345}*
are guaranteed to belong to the same slot.

Note that for collection types such as
[lists](https://redis.io/docs/latest/develop/data-types/lists/) and
[hashes](https://redis.io/docs/latest/develop/data-types/hashes/), or Valkey equivalents, the API
proxy always stores all data belonging to the same collection on the same
shard, corresponding to the hash slot of the collection's key.

Note that like Redis/Valkey Cluster, the API proxy does not support multiple
logical databases, so commands like *SELECT* and *MOVE* are not supported.

### Redis/Valkey Data Types and Commands

The API proxy currently supports a limited subset of Redis/Valkey types:

* Strings
* Lists
* Hashes
* JSON

The API proxy supports most of Redis/Valkey commands for each of the above types,
as described below with Redis links. In addition, it supports most
[Generic commands](https://redis.io/docs/latest/commands/?group=generic) and
some of
[Connection Management commands](https://redis.io/docs/latest/commands/?group=connection)
commands. A limited number of
[Server Management commands](https://redis.io/docs/latest/commands/?group=server)
is also supported.

As in Redis/Valkey, the keys are binary strings (which can be either text or binary).
The key expiration semantics is also supported with millisecond precision.

Below we will describe each of the supported command groups, which commands
are supported and their limitations.

#### Size limitations

Oracle NoSQL API proxy imposes more stringent size limits than are in Redis/Valkey.

The maximum key size is 128KB (vs 512MB in Redis/Valkey). There are also limitations
on maximum size of string values, JSON values, list elements and hash keys and
values described in the sections below.

#### Strings

The API proxy supports most of
[String Commands](https://redis.io/docs/latest/commands/?group=string).

Supported commands:

* GET
* SET
* SETNX
* SETEX
* PSETEX
* GETRANGE
* SUBSTR
* STRLEN
* APPEND
* SETRANGE
* INCR
* INCRBY
* INCRBYFLOAT
* DECR
* DECRBY
* MGET
* MSET
* MSETNX
* GETDEL
* GETSET
* GETEX

Currently not supported commands: *LCS*.

There are following limitations:

1. The size of the string is limited to 256KB (vs 512MB in Redis/Valkey).

2. As noted above, for multikey commands *MGET*, *MSET* and *MSETNX*, all keys
passed to the command must belong to the same hash slot, otherwise *CROSSSLOT*
error is returned. In addition, *MSET* and *MSETNX* allow maximum of 50 keys
to be passed.

3. Redis/Valkey allows to execute unconditional *SET*, *SETEX* and other commands
above that set a value of existing key even if the existing key holds
different type of value. The API proxy allows this as well, but doing so is
problematic when an existing key holds a collection, like a list or a hash,
because the storage for existing collection elements will not be immediately
reclaimed. Instead, the cleanup thread will be run on the next proxy startup
if *-cleanup-on-startup* parameter is *true* (which is the default).

See restriction 3 in [Generic Commands](#generic-commands) section and
*-cleanup-on-startup* in [Command Line Parameters](#command-line-parameters)
section.

In short, executing *SET*, etc. commands on a key holding a collection without
deleting the key first is not recommended.

#### Lists

The API proxy suppports all of
[List Commands](https://redis.io/docs/latest/commands/?group=list).

Supported commands:

* LPUSH
* LPOP
* RPUSH
* RPOP
* LMPOP
* LPUSHX
* RPUSHX
* LLEN
* LREM
* LINDEX
* LRANGE
* LSET
* LTRIM
* LPOS
* LINSERT
* LMOVE
* RPOPLPUSH
* BLPOP
* BRPOP
* BLMPOP
* BLMOVE
* BRPOPLPUSH

The following restrictions apply:

1. The size of each list element is limited to 256KB.

2. Commands that may create, update or delete multiple list elements, such as
all varieties of PUSH and POP, LREM and LTRIM, may only be perform atomically
for maximum of 49 elements. Otherwise, the command is split into multiple
executions affecting 49 or less elements each.

    Examples:

    ```sh
    LPUSH list elem1 elem2 ... elem100
    ```

    is equivalent to executing

    ```sh
    LPUSH list elem1 ... elem49
    LPUSH list elem50 ... elem98
    LPUSH list elem99 elem100
    ```

    ```sh
    LPOP list 100
    ```

    is equivalent to executing

    ```sh
    LPOP list 49
    LPOP list 49
    LPOP list 2
    ```

    The same is for LREM.

    The execution of LTRIM will also be split if more than 49 elements have to be
removed from the list. The splitting favors removing from the tail of the
list first. For example, if the list contains 150 elements (indexes 0 - 149), then

    ```sh
    LTRIM list 70 71
    ```

    is equivalent to executing

    ```sh
    LTRIM list 0 100
    LTRIM list 20 71
    LTRIM list 49 51
    LTRIM list 1 2
    ```

    In the above, we wish to retain only elements 70 and 71. The first command
removes the last 49 elements, the second command removes 29 remaining
elements from the right and 49 - 29 = 20 elements from the left, so that
we are left with 52 elements and the elements we retain are now at indexes
50 and 51. The 3rd command remove first 49 elements from the left and the last
command removes 1 remaining element from the left.

3. Blocking list commands (BLPOP, BRPOP, BLMPOP, BLMOVE, BRPOPLPUSH) use
polling and thus do not guarantee timely return. Exponential backoff algorithm
is used starting with delay of about 200ms and doubling it with small random
interval added. This it is possible that, if no data is available, a blocking
command will wait longer than it takes for data to become available in the
list.

4. For blocking list commands, when multiple clients are waiting on a key,
there is no guaranteed order of unblocking when data arrives. Unlike Redis/Valkey,
which first serves the client that has blocked on a key first, here whichever
client happens to be polling first will get the data.

5. For multi-key commands such as LMOVE, BLMOVE, RPOPLPUSH, BRPOPLPUSH all
provided keys must belong to the same hash slot, otherwise *CROSSSLOT* error is
returned.

6. When LINSERT command is used to insert new element between existing list
elements, on rare occasions this may require reindexing of neighboring list
elements and very rarely elements further away. If there are many clients
accessing the same list concurrently, reindexing may fail, and after several
reindexing attempts an error may be returned to the user. This is a very
remote possibility.

#### Hashes

The API proxy suppports the following
[Hash Commands](https://redis.io/docs/latest/commands/?group=hash):

* HSET
* HMSET
* HDEL
* HLEN
* HGET
* HMGET
* HSCAN
* HKEYS
* HVALS
* HGETALL
* HEXISTS
* HSTRLEN
* HINCRBY
* HINCRBYFLOAT
* HSETNX

Commands not currently supported include all commands dealing with
per-hash-field expiration time: *HEXPIRE*, *HEXPIREAT*, *HEXPIRETIME*, *HTTL*,
*HPERSIST*, *HPEXPIRE*, *HPEXPIREAT*, *HPEXPIRETIME*, *HPTTL*, *HGETEX*,
*HSETEX* (this does not affect per-key expiration time) as well as commands
*HRANDFIELD* and *HGETDEL*.

There are following limitations:

1. The size of each hash entry, which for this purpose is considered as sum of
the size of the field and the size of the value, is limited to 256KB.

2. Commands that may create, update or delete multiple hash entries, such as
HSET, HMSET and HDEL, may only be perform atomically for maximum of 49 elements.
Otherwise, the command is split into multiple executions affecting 49 or less
elements each.

    Example:

    ```sh
    HSET hash field1 value1 field2 value2 ... field100 value100 
    ```

    is equivalent to executing

    ```sh
    HSET hash field1 value1 ... field1 field49 field49
    HSET hash field50 value50 ... field98 value98
    HSET hash field99 value99 field100 value100
    ```

#### JSON

The API proxy supports most of
[JSON Commands](https://redis.io/docs/latest/commands/?group=json).

Supported commands:

* JSON.SET
* JSON.GET
* JSON.ARRAPPEND
* JSON.ARRINSERT
* JSON.ARRPOP
* JSON.ARRTRIM
* JSON.ARRLEN
* JSON.ARRINDEX
* JSON.STRAPPEND
* JSON.STRLEN
* JSON.NUMINCRBY
* JSON.NUMMULTBY
* JSON.TOGGLE
* JSON.TYPE
* JSON.OBJKEYS
* JSON.OBJLEN
* JSON.DEL
* JSON.FORGET
* JSON.CLEAR
* JSON.MGET
* JSON.MERGE
* JSON.MSET

Commands not supported: *JSON.DEBUG*, *JSON.DEBUG MEMORY* and *JSON.RESP*.

The API proxy uses
[JSON Path](https://redis.io/docs/latest/develop/data-types/json/path/) syntax
based on path used in
[Redis JSON](https://redis.io/docs/latest/develop/data-types/json/), with some
limitations:

1. Recursive descent (**..**) is not currently supported.

2. There is limited support for regular expressions inside a filter
expression. In general, regular expressions supported are the ones supported
by
[regex_like SQL function](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/sqlreferencefornosql/regular-expressions.html),
with couple of enhancements:

    * Constructs __^__ and __$__ are supported to match the beginning and end
of the string. Note that unlike *regex_like* function, by default the pattern
would match any substring of the input string, unless __^__ and/or __$__ are
used. Current syntax only allows __^__ and __$__ at the beginning/end of the
pattern correspondingly (not including flags, see below).

    * Some in-line flags are supported, using format **(?flags)**. The flags
supported are the same as described for
[regex_like SQL function](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/sqlreferencefornosql/regular-expressions.html).
The expression **(?flags)** must be at the start of the pattern, before possible **^**.
Example:  _(?isu)^pat.*$_

3. Property identifier following the dot (.) must begin with a letter or
underscore (_) and consist of only letters, digits or underscore. Examples:

    Valid path: *$.abc123_*

    Invalid path: *$.a%*

    For more complex identifiers, use brackets instead. E.g.: *$["a%"]*

4. The syntax that contains dot (.) followed by a bracket is not supported.
E.g.: _$.a.["b"]_ is not supported. Instead, use either _$.a.b_ or _$.a["b"]_

5. Legacy syntax, where paths start either with dot (.) to designate the root
or otherwise don't start with '$' omitting the root, is supported, with
restrictions indicated above in 3. and 4. Bracket can follow dot only if the
dot is the first character designating the root. Examples:

    Valid paths: _.["a"]_, _.a.b[*]_, a.b[1:2]

    Invalid paths: _.a.[*]_, _.a%_, _a.$_, _$$_


Besides JSON Path, there are also following restrictions and differences:

1. There is limitation on maximum size of JSON value stored under a key,
although there is not a definite limit valid for all types of NoSQL service
which can be used by the API proxy. E.g. for Cloud Service, table records
are limited to maximum size of 512KB, so together, the size of the key, the
value and additional meta information cannot exceed this limit, otherwise an
error would be returned.

2. Numeric values allow +/- Infinity and NaN. Like Redis/Valkey JSON, the API proxy
stores numeric JSON values as double precision floating point number. However,
Redis/Valkey JSON disallows non-numeric numbers such as Infinity, -Infinity and NaN,
while the API proxy allows them. In particular, the values of +/- Infinity
may result when storing numeric values outside of double precision range of
approximately +/- 1.79769E+308 or using commands *JSON.NUMINCRBY* and
*JSON.NUMMULTBY* that would result in values outside this range.

3. Like for other multi-key commands, all keys provided to *JSON.MSET* must
belong to the same hash slot, otherwise *CROSSSLOT* error is returned.
In addition, *JSON.MSET* may take maximum of 50 keys.

#### Generic commands

The API proxy suppports the following
[Generic Commands](https://redis.io/docs/latest/commands/?group=generic):

* COPY
* DEL
* RENAME
* RENAMENX
* EXISTS
* TYPE
* SCAN
* KEYS
* PEXPIRETIME
* EXPIRETIME
* PTTL
* TTL
* PEXPIRE
* PEXPIREAT
* EXPIRE
* EXPIREAT
* PERSIST

There are following restrictions:

1. For commands that may take multiple keys, like *COPY*, *DEL*, *RENAME*,
*RENAMENX* and *EXISTS*, all keys must belong to the same hash slot, otherwise
*CROSSSLOT* error will be returned.

2. *DEL* command can delete maximum of 50 keys atomically. If more than 50
keys are provided, the command will be split into multiple atomic operations
of 50 keys or less.

3. When commands such *DEL*, *RENAME* and *RENAMENX* and *COPY* operate on
keys containing collections, such as lists and hashes, the deletion or copying
of collection elements is not done atomically with the creation and/or
deletion of the keys themselves. This does not affect concurrency or data
integrity because the elements are bound to their keys via unique id (UUID).
However, if the API proxy was terminated in the middle of such operation
(e.g. deleting or copying of list elements), some stale data may remain in the
database. The API proxy will try to cleanup such data at startup. This is
controlled by command line parameter *-cleanup-on-startup* when starting the
proxy, as described in [Command Line Parameters](#command-line-parameters)
section.

4. When using *SCAN* command with *MATCH* option or *KEYS* command with
*pattern* parameter, some more advanced glob patterns may not be supported.
For example, reverse ranges like *[z-a]* are not supported.

5. For *SCAN* command, you may notice large integer values used as the cursor
values, unlike small integer values usually shown in examples of *SCAN*
command.

#### Connection Management commands

The API proxy supports limited number of
[Connection Management Commands](https://redis.io/docs/latest/commands/?group=connection):

* PING
* ECHO
* QUIT

Commands *HELLO* and *CLIENT* are partially supported for testing purposes,
but should not be used by applications.

#### Server Management Commands

The API proxy supports the following
[Server Management Commands](https://redis.io/docs/latest/commands/?group=server):

* DBSIZE
* TIME

Commands *CONFIG* and *INFO* are partially supported for testing purposes, but
should not be used by applications.

## NoSQL Database Schema and Data Format

The API proxy uses one parent table to store Redis/Valkey keys and values as well
as two child tables to store elements of collections, for lists and hashes
correspondingly. The tables and indexes are created the first time the API
proxy connects to the database.

### On Encoding of Values

In Redis/Valkey, both keys and values are binary strings, which means they can store
both text and arbitrary binary data. Even though, using text for keys and/or
values is more common.

The API proxy stores all keys and values as UTF-8 strings. This also
includes list elements and hash fields and values. UTF-8 text values are
stored as is. Binary values are stored as base-64 encoding of the value. To
distinguish between these cases, a 1-character prefix is used, 'T' for text
and 'B' for binary.

For example:

* String "abcde" will be stored as "Tabcde".
* Binary value *00 00 00 00* will be stored as "BAAAAAA==".

### Main Valkey Table

The main Valkey table is created as following:

```sql
CREATE TABLE valkey(slot INTEGER, id STRING, key JSON, value JSON,
  PRIMARY KEY(SHARD(slot), id));
```

Each row of this table stores information for a Redis/Valkey key-value pair:

* Short primary key uniquely identifying the Redis/Valkey key.
* The Redis/Valkey key itself.
* The value, which stores both the data type and the data. The content of the
data depends on its data type. For collection types such as lists and hashes,
this value stores the header of the collection. The elements of the collection
are stored in separate child tables.

The columns are:

1. slot - key hash slot. See
[Redis Cluster Key Distribution Model](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#key-distribution-model).

    The slot is computed as CRC16(key) mod 16384. Note that the slot also serves
as a shard key for this table. This means that keys that hash to the same
hash slot will always be stored on the same shard and thus may be part of the
same atomic operation.

2. id - id used to uniquely identify the key. Since slot and id serve as
primary key and the primary key can be maximum of 64 bytes, we cannot always
use the Redis/Valkey key itself (even encoded) as the value for id. Instead, the id
is determined as follows:

    The encoded value of the key is computed as described in
[On Encoding of Values](#on-encoding-of-values). If the resulting size is no
greater than 60 bytes (this is because 4 bytes are also needed to store the
slot), the resulting value will be stored as id. Otherwise, we compute
SHA-256 digest, encoded as base-64 and prefixed with prefix 'H' to serve as
the value of id (thus id will always start with one of the prefixes 'T', 'B'
or 'H').

3. key - JSON object in the form:

    ```json
    {
      "data": <encoded key>,
      "scanId": <scan id>,
      "exp": <expiration timestamp>
    }
    ```

    where:

    * "data" is a string that stores the full encoded value of the Redis/Valkey key,
as described in [On Encoding of Values](#on-encoding-of-values) (not the
SHA-256 digest).
    * "scanId" is 64-bit integer value that identifies the key for the purpose
of the *SCAN* command (this value is needed to maintain the state between
multiple invokations of the *SCAN* command, because the cursor used in SCAN
is 64-bit integer).
    * "exp" is 64-bit integer which is the key expiration time as Unix
timestamp in milliseconds. This field is optional and only present if the key
expiration was set by commands *SET*, *EXPIRE*, *EXPIREAT*, *PEXPIRE*,
*PEXPIREAT*, etc.

4. value - JSON object that stores the value for the key. The exact format
depends on the type of value. Each value object has a "type" field that
designates the type of value. Currently supported values for type field are:

    _string_, _list_, _hash_, _ReJSON-RL_ (the last one mimics the type name
used by Redis/Valkey JSON).

    1) For string values the format of value object is:

        ```json
        {
          "type": "string",
          "data": <encoded value>
        }
        ```

        where "data" is a string that stores the value encoded as specified in
[On Encoding of Values](#on-encoding-of-values)

    2) For JSON values the format of value object is:

        ```json
        {
          "type": "ReJSON-RL",
          "json": <JSON value>,
          "pad": <used internally>
        }
        ```

        where "json" is a value of NoSQL datatype JSON that stores the represented
JSON value.

    We will describe the format of value for lists and hashes in the next section.

#### Secondary indexes

There is an index on *scanId* to improve performance of *SCAN* command:

```sql
CREATE INDEX scanIdIdx ON valkey(key.scanId AS LONG);
```

### Child Collection Tables

Because collections like lists and hashes can become very big and store many
elements, it does not scale well to store the whole collection in one row of
the main *valkey* table. Instead, the collection elements are stored in
separate tables *valkey.lists* and *valkey.hashes* which are child tables of the
main *valkey* table. Each collection element (list element or hash field-value)
is stored in its own row.

Note that *valkey.lists* stores elements for all lists and *valkey.hashes*
stores elements for all hashes. Since child table's primary key includes
parent primary key, this uniquely identifies the Redis/Valkey key of the collection
to which an element belongs.

One particular column that is present in both child tables is *cid*
(collection id), which is a UUID string. This serves as a unique mapping
between a collection element (row in the child table) and the particular
collection (stored as a row in a parent table) to which the element belongs.
This mapping is achived by also storing the same *cid* string as part of
*value* object described in the previous section. This mapping is needed (even
though there is already a parent key mapping) because operations like *DEL*,
*COPY* and *RENAME* cannot be done atomically on large collections and the
API proxy must avoid data corruption in scenarios when a Redis/Valkey key holding a
collection is deleted and the same key is recreated (which may also hold a
collection of the same type or a different value), which could be done by
multiple Redis/Valkey clients in a concurrent environment. Using UUID ensures unique
mapping since UUID will not be reused even if a collection is deleted and new
collection is created with the same Redis/Valkey key.

Note that although using UUID protects data integrity, a "garbage" data may be
left over in *valkey.lists* or *valkey.hashes* if the proxy is terminated in the
middle of *DEL*, *COPY* or *RENAME* operation, or if one of unconditional
*SET* commands is executed on a key holding a collection (doing this is not
recommended). See *-cleanup-on-startup* parameter in
[Command Line Parameters](#command-line-parameters) section.

#### Lists

For Redis/Valkey key holding a list, the *value* column of the main *valkey* table
has the following format:

```json
{
  "type": "list",
  "cid": <list's cid>,
  "len": <list's size>
}
```

where:

* "cid" - cid string used for mapping between particular collection and its
elements as explained above.
* "len" - 64-bit integer that stores the number of elements in the list.

*valkey.lists* child table is created as follows:

```sql
CREATE TABLE valkey.lists(elemId NUMBER, cid STRING AS UUID, value STRING,
  PRIMARY KEY(elemId));
```

The columns are:

1. elemId - numeric value that keeps order between list elements.

2. cid - see explanation above

3. value - the element value as a string encoded as specified in
[On Encoding of Values](#on-encoding-of-values).

#### Hashes

Hashes, as collections of field-value pairs, can represent objects, so using
a small hash (with just a few field-value pairs) can be a very common use
case. For this reason, API proxy optimizes storage for small hashes by
storing them inline, inside the *value* field of the main *valkey* table. If
the hash grows to exceed certain threshold size (number of elements), it is
automatically converted to multi-row format where the *value* field of the
main *valkey* table only stores hash header and the elements (field-value
pairs) are stored in *valkey.hashes* child table, one per row.

The coversion takes place when the size exceeds threshold size of 48
field-value pairs (chosen as such because it is maximum size at which the
conversion can be done atomically). The conversion also happens if any
field-value pair exceeds certain size, currently chosen as 10 Kb (calculated
as sum of lenghs of encoded field and value). Note that this is no backward
conversion, i.e. if the hash becomes smaller it is not converted back to the
inline format.

For Redis/Valkey key holding a hash, the *value* column of the main *valkey* table
has the following format:

```json
{
  "type": "hash",
  "cid": <hash's cid>,
  "len": <hash's size>,
  "smallVal": <inline value>
}
```

where:

* "cid" - cid string used for mapping between particular collection and its
elements as explained above.
* "len" - 64-bit integer that stores the number of entries in the hash. This
field is present only if the hash is not stored inline (see above).
* smallVal - value of the hash stored as JSON object. This field is present
only if the hash is stored inline as described above. We will describe the
format of this field below.

Note that fields *len* and *smallVal* are mutually exclusive as desribed
above.

*valkey.hashes* child table is used to store hash entries (field-value pairs),
one per row, for hashes stored in regular multi-row format (not inline). It is
created as follows:

```sql
CREATE TABLE valkey.hashes(keyId STRING, cid STRING AS UUID, key JSON,
  value STRING, PRIMARY KEY(keyId));
```

The columns are:

1. keyId - id to uniquely identify hash entry in the given hash. It is
analogous to the *id* column of the main *valkey* table and is computed in the
same way as described for *id* column in [Main VAlkey Table](#main-valkey-table)
section from the field of the entry.

2. cid - see explanation in the beginning of
[this section](#child-collection-tables).

3. key - stores the field of the entry. This is analogous to the *key* column
of the main *valkey* table, described in [Main Valkey Table](#main-valkey-table)
section. It is a JSON object in the following format:

    ```json
    {
      "data": <encoded field>,
      "scanId": <scan id>
    }
    ```

    where "data" is the hash field stored as string encoded as specifed in
[On Encoding of Values](#on-encoding-of-values) and "scanId" is 64-bit
integer used for *HSCAN* command, analogous to "scanId" field of key column
described in [Main Valkey Table](#main-valkey-table) section. "exp" field may
also be added in future to support per-hash-entry expiration.

4. value - the value as a string encoded as specified in
[On Encoding of Values](#on-encoding-of-values).

##### Secondary indexes

There is an index on *key.scanId* to improve performance of *HSCAN* command:

```sql
CREATE INDEX hScanIdIdx ON valkey.hashes(key.scanId AS LONG);
```

Going back to the case when the hash is stored inline in the main *valkey*
table (and *valkey.hashes* is not used), the format of *smallVal* field of the
*value* column mimics *keyId*, *key* and *value* columns of *valkey.hashes*
table. The format of *smallVal* is as follows:

```json
{
  "keyId1": {
    "key": <key1>,
    "value": <value1>
  },
  "keyId2": {
    "key": <key2>,
    "value": <val2>
  },
  ...
}
```

Each field of *smallVal* is the key id of the hash entry, the value of which
is an object containing fields "key" and "value" for corresponding hash entry.
The format for "keyId...", "key" and "value" fields in *smallVal* is the same
as described above for *keyId*, *key* and *value* columns of *valkey.hashes*.

### JSON Format

As mentioned, JSON values are stored in "json" field of *value* column in the
main *valkey* table. One caveat concerns the storage of JSON arrays. For
technical reasons, each array element is stored encapsulated into an object
with field "v" storing the element value. E.g. JSON value

```json
{
  "a": [1, 2, 3, 4, 5]
}
```

will be stored as

```json
{
  "a": [{ "v": 1 }, { "v": 2 }, { "v": 3 }, { "v": 4 }, { "v": 5 }]
}
```

This applies to all arrays within the JSON value. The API proxy will
transparently convert between this and regular representation, so Redis/Valkey
clients will not be aware of this. This format is only seen if examining the
table data directly.
