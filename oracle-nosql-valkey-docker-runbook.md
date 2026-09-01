# Oracle NoSQL Valkey/Redis API Adapter: Docker Runbook

The Oracle NoSQL Valkey/Redis API Adapter lets standard Redis and Valkey clients use Oracle NoSQL Database as a Redis-compatible data store. The adapter is an API proxy: it accepts Redis/Valkey commands and persists the corresponding data in Oracle NoSQL Database.

This runbook uses the Oracle NoSQL Community Edition (KVLite) container described in [Oracle's NoSQL Docker image README](https://github.com/oracle/docker-images/tree/main/NoSQL), then runs the adapter and connects with either Redis CLI or Valkey CLI.

## Prerequisites

- Docker or Podman with Docker-compatible commands
- Oracle NoSQL Community Edition image
- Oracle NoSQL Valkey API Adapter image
- A Redis or Valkey CLI image, either official or built locally

## 1. Obtain Oracle NoSQL Community Edition

This container image uses a simplified version of the Oracle NoSQL Database called KVLite. KVLite runs as a single process that provides a single storage node and single storage shard. KVLite does not include replication or administration.

> **Note:** KVLite is **not** intended for production deployment or performance measurements. Test only with data that is not sensitive; do not use usernames, passwords, credit-card details, medical information, or other sensitive data.

> **Note:** Two container images are available: one with a secure configuration and one with a non-secure configuration. They differ primarily in how KVLite access is performed. The secure setup is recommended, although it requires additional setup steps and provides useful exposure to secure KVStore configuration.

Pull and tag the KVLite image:

```bash
docker pull ghcr.io/oracle/nosql:latest-ce
docker tag ghcr.io/oracle/nosql:latest-ce oracle/nosql:ce
```

## 2. Obtain or build the API Adapter

Pull and tag the adapter image:

```bash
docker pull ghcr.io/oracle/nosql-valkey-api:latest
docker tag ghcr.io/oracle/nosql-valkey-api oracle/nosql-valkey-api
```

To build the adapter from its source repository instead:

```bash
mvn -f ./java/pom.xml clean package && \
docker build -t oracle/nosql-valkey-api:latest .
```

## 3. Obtain or build a Redis/Valkey CLI image

Choose one CLI implementation before connecting it to the API Adapter.

### Use an official image

Docker/Podman pulls these automatically when a CLI command is first run. Pull them explicitly if you prefer:

```bash
docker pull redis:latest
docker pull valkey/valkey:9.1.1
```

### Build a local CLI-only image

To build `localhost/redis-cli` or `localhost/valkley-cli`, use the Dockerfiles and build commands in [Appendix: locally built CLI images](#appendix-locally-built-cli-images).

## 4. Choose a networking model

| Option | CLI connection | CLI network flag |
| --- | --- | --- |
| 1. Shared network namespace | `127.0.0.1:6379` | `--network container:nosql-valkey-api` |
| 2. Named network | `nosql-valkey-api:6379` | `--network nosql-net` |

### Wait for service readiness

KVLite creates its store and starts its HTTP proxy asynchronously. Run this helper once, then use it after starting each service:

```bash
wait_for_log() {
  local container="$1" pattern="$2"
  for _ in {1..60}; do
    if docker logs "$container" 2>&1 | grep -q "$pattern"; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $container: $pattern" >&2
  return 1
}
```

## Option 1: Shared API network namespace

Use this when the API Adapter keeps its default listener address, `127.0.0.1:6379`. The CLI shares the API container's network namespace, so its own `127.0.0.1` reaches the adapter. `container:nosql-valkey-api` is a special Docker/Podman network mode, not the name of a Docker network.

```bash
docker run -d --rm --name kvlite --hostname kvlite \
  --env KV_PROXY_PORT=8080 \
  -p 8080:8080 \
  oracle/nosql:ce

wait_for_log kvlite 'Proxy started:'

docker run -d --rm --name nosql-valkey-api \
  -p 6379:6379 \
  oracle/nosql-valkey-api:latest \
  -auth kvstore \
  -endpoint http://host.docker.internal:8080

wait_for_log nosql-valkey-api 'Started Valkey API Proxy'
```

### Connect with an official CLI image

Use Redis CLI:

```bash
docker run -it --rm --network container:nosql-valkey-api redis:latest \
  redis-cli -h 127.0.0.1 -p 6379
```

Use Valkey CLI:

```bash
docker run -it --rm --network container:nosql-valkey-api valkey/valkey:9.1.1 \
  valkey-cli -h 127.0.0.1 -p 6379
```

### Connect with a locally built CLI image

Use the locally built Redis CLI image:

```bash
docker run -it --rm --network container:nosql-valkey-api --entrypoint redis-cli localhost/redis-cli \
  -h 127.0.0.1 -p 6379
```

Use the locally built Valkey CLI image:

```bash
docker run -it --rm --network container:nosql-valkey-api --entrypoint valkey-cli localhost/valkley-cli \
  -h 127.0.0.1 -p 6379
```

### Verify the connection

At the Redis or Valkey CLI prompt:

```text
PING
SET greeting hello
GET greeting
```

Expected responses include `PONG`, `OK`, and `hello`.

## Option 2: Named Docker network

Use this for direct container-to-container networking. The API Adapter binds to the `nosql-valkey-api` hostname. Without `-host`, its default `127.0.0.1` listener is inaccessible to other containers.

```bash
docker network create nosql-net

docker run -d --rm --name kvlite --hostname kvlite \
  --network nosql-net \
  --env KV_PROXY_PORT=8080 \
  oracle/nosql:ce

wait_for_log kvlite 'Proxy started:'

docker run -d --rm --name nosql-valkey-api --hostname nosql-valkey-api \
  --network nosql-net \
  oracle/nosql-valkey-api:latest \
  -host nosql-valkey-api \
  -auth kvstore \
  -endpoint http://kvlite:8080

wait_for_log nosql-valkey-api 'Started Valkey API Proxy'
```

### Connect with an official CLI image

Use Redis CLI:

```bash
docker run -it --rm --network nosql-net redis:latest \
  redis-cli -h nosql-valkey-api -p 6379
```

Use Valkey CLI:

```bash
docker run -it --rm --network nosql-net valkey/valkey:9.1.1 \
  valkey-cli -h nosql-valkey-api -p 6379
```

### Connect with a locally built CLI image

Use the locally built Redis CLI image:

```bash
docker run -it --rm --network nosql-net --entrypoint redis-cli localhost/redis-cli \
  -h nosql-valkey-api -p 6379
```

Use the locally built Valkey CLI image:

```bash
docker run -it --rm --network nosql-net --entrypoint valkey-cli localhost/valkley-cli \
  -h nosql-valkey-api -p 6379
```

### Verify the connection

At the Redis or Valkey CLI prompt:

```text
PING
SET greeting hello
GET greeting
```

Expected responses include `PONG`, `OK`, and `hello`.

## 5. Troubleshooting

### Rootless Podman `slirp4netns` and systemd warning

When `docker` prints `Emulate Docker CLI using podman`, it is invoking Podman. In a rootless Podman environment, this message can appear when Podman cannot move its `slirp4netns` network-helper process into the systemd user cgroup:

```text
failed to move the rootless netns slirp4netns process to the systemd user.slice
```

This warning can be ignored when the command still returns a container ID, `docker ps` shows the container as running, and the subsequent `wait_for_log` check succeeds. The helper process is still able to provide networking in that case. It reflects a systemd user-session/cgroup placement issue, not an Oracle NoSQL or API Adapter failure.

If the container does not start, networking fails, or the readiness check times out, treat it as a Podman host-configuration issue and inspect the container with:

```bash
docker ps -a --filter name=kvlite
docker logs kvlite
```

See the related [rootless Podman networking issue](https://github.com/containers/podman/issues/22934) for an example of a case where rootless networking genuinely fails.

## 6. Clean up

Stop and remove the KVLite and API Adapter containers:

```bash
docker rm -f nosql-valkey-api kvlite
```

Remove the named network used by Option 2:

```bash
docker network rm nosql-net
```

## Appendix: locally built CLI image recipes

Use these recipes only when you want locally built CLI-only images instead of the official images.

### Redis CLI image

`Dockerfile.redis-cli`:

```dockerfile
ARG ORACLE_LINUX_VERSION=10
FROM oraclelinux:${ORACLE_LINUX_VERSION}

RUN yum -y install openssl-devel gcc make curl tar

RUN cd /tmp && \
    curl -fsSL http://download.redis.io/redis-stable.tar.gz | tar xz && \
    make -C redis-stable && \
    cp redis-stable/src/redis-cli /usr/local/bin && \
    rm -rf /tmp/redis-stable

ENTRYPOINT ["redis-cli"]
```

```bash
docker build -f Dockerfile.redis-cli -t localhost/redis-cli .
```

### Valkey CLI image

`Dockerfile.valkley-cli`:

```dockerfile
FROM oraclelinux:10

ARG VALKEY_VERSION=9.1.1

RUN yum -y install openssl-devel gcc make curl tar && \
    cd /tmp && \
    curl -fsSL "https://github.com/valkey-io/valkey/archive/refs/tags/${VALKEY_VERSION}.tar.gz" | tar xz && \
    make -C "valkey-${VALKEY_VERSION}" && \
    cp "valkey-${VALKEY_VERSION}/src/valkey-cli" /usr/local/bin && \
    rm -rf "/tmp/valkey-${VALKEY_VERSION}"

ENTRYPOINT ["valkey-cli"]
```

```bash
docker build -f Dockerfile.valkley-cli -t localhost/valkley-cli .
```

## References

- [Oracle NoSQL Database on Docker](https://github.com/oracle/docker-images/tree/main/NoSQL)
- [Oracle NoSQL Database documentation](https://docs.oracle.com/en/database/other-databases/nosql-database/)
- [Valkey official container image](https://valkey.io/)
- [Redis Docker Official Image](https://hub.docker.com/_/redis/)
