# Apache Flink 101

This repository is for the exericses in the [Apache Flink 101](https://developer.confluent.io/courses/apache-flink/intro/) course hosted on Confluent Developer.

It uses Docker Compose to start up Kafka, Flink, and Flink's SQL CLI.

## Launch

First build the image and start all of the containers:

```bash
docker compose up --build -d
```

Kafka is available to host applications at `localhost:9092`, and Kafka UI is available at <http://localhost:8080>.
Containers attached to this Compose network should connect to `broker:29092`. A container outside this Compose
network can connect to `host.docker.internal:9092`; on Linux, add
`--add-host=host.docker.internal:host-gateway` to that container. If the host uses another reachable address,
set `KAFKA_EXTERNAL_HOST` to that address before starting Compose.

Once the containers are running,

```bash
docker compose run sql-client
```

will drop you into the Flink SQL Client, where you can interact with Flink SQL.

## Run the Java demo

The Java demo is in the `demo` directory. Run it in a separate terminal:

```bash
cd demo
mvn exec:exec -X
```

The demo continuously generates transactions and prints the hourly totals. Press `Ctrl+C` to stop it.

## Shutdown

When you're done, this will shutdown all of the containers and delete the volume that was created for checkpointing:

```bash
docker compose down -v
```
