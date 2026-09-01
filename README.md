# Oracle NoSQL Valkey/Redis API Adapter

Oracle NoSQL Valkey/Redis API Adapter enables applications to use Oracle NoSQL Database as a Redis-compatible data store.
It supports standard Valkey/Redis clients and Valkey/Redis commands while persisting those data structures in Oracle NoSQL Database.
The adapter (API proxy) can be run as a Docker container, a standalone Java application, or embedded within Java applications.

## Installation

### Prerequisites

- Oracle NoSQL Database (Cloud Service, On-Premise, or Cloud Simulator)
- Docker or Java 11+
- Valkey/Redis CLI or any supported Valkey/Redis client

### Install with Docker

```bash
docker pull ghcr.io/oracle/nosql-valkey-api:latest
docker tag ghcr.io/oracle/nosql-valkey-api oracle/nosql-valkey-api
```

### Install with Maven

```xml
<dependency>
  <groupId>com.oracle.nosql.valkey</groupId>
  <artifactId>nosql-valkey</artifactId>
  <version>latest-version</version>
</dependency>
```

After installation, start the API Proxy using Docker or Java and connect with any compatible client.

## Quick start: building the container image

This assume you have cloned this repository and are in the root directory.

To build a container image named oracle/nosql-valkey-api:latest use:

````bash
mvn -f ./java/pom.xml clean package && docker build -t oracle/nosql-valkey-api:latest .
````


## Documentation

For a reproducible local Docker workflow with KVLite, the API Adapter, and
Redis/Valkey CLI containers, see
[oracle-nosql-valkey-docker-runbook.md](oracle-nosql-valkey-docker-runbook.md).

For complete developer documentation, including configuration, command-line
parameters, supported environments, and Java usage, see
[README-DEV.md](README-DEV.md).

For Oracle NoSQL Database and Proxy install and configuration, see the Oracle NoSQL Database documentation:
- https://docs.oracle.com/en/database/other-databases/nosql-database/



## Examples

The project includes examples demonstrating how to:

- Start Oracle NoSQL Database
- Run the API Proxy
- Connect using 'valkey-cli'/`redis-cli`
- Execute common Valkey/Redis commands, including:
  - Strings (`SET`, `GET`)
  - Lists
  - Hashes
  - JSON operations
 
## Help

For Oracle NoSQL Database documentation and support, visit:

- https://docs.oracle.com
- https://www.oracle.com/database/technologies/nosql.html

If enabled, issues and feature requests can be submitted through this repository's GitHub Issues page.

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## License

Copyright (c) 2026 Oracle and/or its affiliates.

Released under the Universal Permissive License (UPL) Version 1.0 as shown at https://oss.oracle.com/licenses/upl/.
