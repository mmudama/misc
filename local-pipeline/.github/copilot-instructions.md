**Purpose**
- **Brief:** This project demonstrates a small end‑to‑end data pipeline using Kafka, Flink, Avro, and Parquet. Each component runs in its own Docker container. The data stream is produced by polling `/proc/stat` inside the `producer` container and publishing those snapshots to a Kafka topic.

**Pipeline Architecture**

**Big Picture**
- **Components:** Minimal streaming pipeline using Kafka (broker), Schema Registry, a containerized producer, and a Java-based Flink consumer.
- **Orchestration:** All services run via `docker-compose.yml`, which defines `kafka`, `schema-registry`, `producer`, `jobmanager`, and `taskmanager`. Two custom images are used: `local/procstat-producer:latest` and `local/flink-kafka:1.19.3`.

**Assumptions**
The Makefile requires a Posix-compatible (Unix) shell. It's been tested on WSL Ubuntu on Windows.

**Data Flow & Key Files**

- **Source:** `/proc/stat` is parsed by the containerized producer (`bin/producer_procstat_avro_container.py`).
- **Schema:** Avro schema lives at `bin/procstat_schema.avsc`. The producer registers it automatically.
- **Kafka Topics:**
  - `procstat_snapshots`: raw /proc/stat snapshots in Confluent Avro.
  - `procstat_metrics`: CPU utilization deltas in JSON.
- **Consumer:** Java Flink job (`CpuEnrichmentConsumer.java`) that:
  - reads Avro from `procstat_snapshots`
  - computes CPU utilization deltas
  - uses keyed state (keyed by hostname)
  - writes JSON metrics to `procstat_metrics`

**Project-specific conventions & patterns**

- **Schema-first Avro:** Producer uses Confluent Avro serializers with automatic schema registration.
- **Containerized producer:** Built from a custom Docker image with all Python dependencies and the Avro schema baked in.
- **Flink deployment:** Runs from a custom Flink image that includes Kafka connector JARs, baked configuration, and an auto-submit entrypoint.
- **Java consumer:** Maven project (`flink/java-consumer/`) builds a shaded JAR with all dependencies included.

**Integration points & dependencies**

- **Confluent stack:** Kafka and Schema Registry (Confluent 7.5.x) running in KRaft mode.
- **Producer:** Python 3.11 with `confluent-kafka`, `fastavro`, and other lightweight utilities baked into the image.
- **Flink consumer:** Maven-based Java project using Flink 1.19.x, Kafka connector 3.x, and Confluent Avro serializers.
- **Build tools:** `make` for orchestration and `mvn` for Java builds.
- **Healthchecks:** Kafka and Schema Registry expose healthchecks; the producer waits for both before starting.

**Common constraints**

- The consumer JAR must be available under `/opt/flink/usrlib/` (volume-mapped from `./flink/`).
- Kafka topics are created during `make setup`; code should not assume auto-creation.

**Architecture notes**
- Configuration is baked into Docker images; Flink properties are not injected via environment variables.
- The Flink job auto-submits on startup; no manual `flink run` steps.
- Healthchecks define startup ordering for Kafka, Schema Registry, and the producer.
- The consumer JAR is volume-mounted so it can be updated without rebuilding the Flink image.


**Design decisions & gotchas:**

- **Configuration must be baked into the Dockerfile.**
  Flink does not reliably parse multi-line config from environment variables. Always write configuration into `flink-conf.yaml` during image build.

- **Explicit memory settings are required.**
  Flink 1.19.3 fails without `jobmanager.memory.process.size` and `taskmanager.memory.process.size`. We chose 1600m (JM) and 1728m (TM).

- **The entrypoint must wait ~20 seconds before job submission.**
  TaskManager slot registration is delayed; removing this wait causes “no available slots” errors.

- **The producer must be containerized with dependencies baked in.**
  Avoid runtime `pip install`. The producer image also handles schema registration automatically.

- **Use a custom Flink image.**
  Include Kafka connector JARs, baked configuration, and an auto-submit entrypoint that runs the job when JobManager becomes ready.




