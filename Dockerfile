# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
#

FROM ghcr.io/graalvm/jdk-community:25

ARG JAR_VER=0.1.0

# OCI Image Metadata
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION=${JAR_VER}

LABEL org.opencontainers.image.title="Oracle NoSQL Redis Proxy" \
      org.opencontainers.image.description="Redis protocol proxy for Oracle NoSQL Database" \
      org.opencontainers.image.vendor="Oracle" \
      org.opencontainers.image.authors="Oracle" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="UPL-1.0" \
      org.opencontainers.image.source="https://github.com/<org>/<repo>" \
      org.opencontainers.image.url="https://github.com/<org>/<repo>" \
      org.opencontainers.image.documentation="https://github.com/<org>/<repo>/README.md"

RUN groupadd --system oracle \
 && useradd --system --gid oracle --create-home --home-dir /redis redis

WORKDIR /redis

ENV NOSQL_REDIS_PROXY_IN_CONTAINER=true

COPY --chown=redis:oracle \
    java/target/nosql-redis-${JAR_VER}-jar-with-dependencies.jar \
    /redis/nosql-redis-jar-with-dependencies.jar

USER redis:oracle

EXPOSE 6379

ENTRYPOINT [ \
  "java", \
  "-cp", \
  "/redis/nosql-redis-jar-with-dependencies.jar", \
  "oracle.nosql.redis.NoSQLRedisServer" \
]
