FROM ocr-docker-remote.artifactory.oci.oraclecorp.com/java/openjdk:21.0.2-oraclelinux8
RUN groupadd oracle && useradd redis -m -g oracle
USER redis:oracle
WORKDIR /redis
ARG JAR_VER=0.1.0
ENV NOSQL_REDIS_PROXY_IN_CONTAINER=true
COPY java/target/nosql-redis-${JAR_VER}-jar-with-dependencies.jar \
	/redis/nosql-redis-jar-with-dependencies.jar
EXPOSE 6379
ENTRYPOINT [ "java", "-cp", \
			"/redis/nosql-redis-jar-with-dependencies.jar", \
			"oracle.nosql.redis.NoSQLRedisServer" ]
