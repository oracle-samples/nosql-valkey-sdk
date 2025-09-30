FROM ocr-docker-remote.artifactory.oci.oraclecorp.com/java/openjdk:21.0.2-oraclelinux8
RUN groupadd oracle && useradd redis -m -g oracle
USER redis:oracle
WORKDIR /redis
ARG REDIS_JAR=oracle-nosql-redis-jar-with-dependencies.jar
ENV NOSQL_REDIS_PROXY_IN_CONTAINER=true
COPY java/target/oracle-nosql-redis-jar-with-dependencies.jar /redis
EXPOSE 6379
ENTRYPOINT ["java", "-cp", \
"/redis/oracle-nosql-redis-jar-with-dependencies.jar", \
"oracle.nosql.redis.NoSQLRedisServer" ]
