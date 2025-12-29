.PHONY: all setup clean create-kafka-topic create-metrics-topic register-schema java-consumer docker-images check-kafka check-schema-registry help

# Default target: build everything (run BEFORE docker compose up)
all: docker-images java-consumer

# Setup target: configure runtime (run AFTER docker compose up)
setup: create-kafka-topic create-metrics-topic register-schema

# Help target
help:
	@echo "Available targets:"
	@echo "  all                  - Build Docker images and Java consumer (run BEFORE 'docker compose up')"
	@echo "  setup                - Create Kafka topics and register schema (run AFTER 'docker compose up')"
	@echo "  docker-images        - Build custom Docker images (producer and Flink)"
	@echo "  java-consumer        - Build Java consumer JAR"
	@echo "  create-kafka-topic   - Create procstat_snapshots topic (requires containers running)"
	@echo "  create-metrics-topic - Create procstat_metrics topic (requires containers running)"
	@echo "  register-schema      - Register Avro schema with Schema Registry (requires containers running)"
	@echo "  clean                - Remove build artifacts"
	@echo "  help                 - Show this help message"
	@echo ""
	@echo "Typical workflow:"
	@echo "  1. make all              # Build everything"
	@echo "  2. docker compose up -d  # Start containers"
	@echo "  3. make setup            # Configure Kafka topics and schema"

# Build Docker images if Dockerfiles have changed
docker-images: docker_producer_image/Dockerfile docker_flink_image/flink-kafka.Dockerfile
	@echo "Building custom Docker images..."
	docker compose build producer jobmanager taskmanager
	@echo "Docker images built successfully."

# Build Java consumer JAR if source files have changed
java-consumer: flink/procstat-flink-consumer-0.1.0.jar

flink/procstat-flink-consumer-0.1.0.jar: flink/java-consumer/pom.xml flink/java-consumer/src/main/java/local/pipeline/CpuEnrichmentConsumer.java flink/java-consumer/src/main/java/local/pipeline/AvroToJsonDeserializationSchema.java
	@echo "Building Java consumer JAR..."
	cd flink/java-consumer && mvn clean package
	cp flink/java-consumer/target/procstat-flink-consumer-0.1.0.jar flink/
	@echo "Java consumer JAR built successfully."

# Create procstat_snapshots Kafka topic (only if Kafka is running)
create-kafka-topic: check-kafka
	@echo "Checking if Kafka topic exists..."
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q '^procstat_snapshots$$' && echo "Topic 'procstat_snapshots' already exists." || ( \
		echo "Creating Kafka topic 'procstat_snapshots'..." && \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic procstat_snapshots --partitions 1 --replication-factor 1 && \
		echo "Topic created successfully." \
	)

# Create procstat_metrics Kafka topic (only if Kafka is running)
create-metrics-topic: check-kafka
	@echo "Checking if metrics topic exists..."
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q '^procstat_metrics$$' && echo "Topic 'procstat_metrics' already exists." || ( \
		echo "Creating Kafka topic 'procstat_metrics'..." && \
		docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic procstat_metrics --partitions 1 --replication-factor 1 && \
		echo "Metrics topic created successfully." \
	)

# Check if Kafka container is running
check-kafka:
	@docker ps --filter name=kafka --filter status=running --format '{{.Names}}' | grep -q kafka || ( \
		echo "ERROR: Kafka container is not running." && \
		echo "Please run 'docker compose up -d' first." && \
		exit 1 \
	)

# Check if Schema Registry container is running
check-schema-registry:
	@docker ps --filter name=schema-registry --filter status=running --format '{{.Names}}' | grep -q schema-registry || ( \
		echo "ERROR: Schema Registry container is not running." && \
		echo "Please run 'docker compose up -d' first." && \
		exit 1 \
	)

# Register Avro schema with Schema Registry
register-schema: check-schema-registry bin/procstat_schema.avsc
	@echo "Checking if schema is already registered..."
	@curl -s http://localhost:8081/subjects/procstat_snapshots-value/versions | grep -q '\[' && echo "Schema already registered." || ( \
		echo "Registering schema..." && \
		schema=$$(cat bin/procstat_schema.avsc | jq -Rs '.') && \
		curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
			--data "{\"schema\":$$schema}" \
			http://localhost:8081/subjects/procstat_snapshots-value/versions && \
		echo "" && echo "Schema registered successfully." \
	)

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf flink/java-consumer/target 2>/dev/null || true
	@rm -f flink/procstat-flink-consumer-0.1.0.jar 2>/dev/null || true
	@rm -f flink/java-consumer/dependency-reduced-pom.xml 2>/dev/null || true
	@echo "Clean complete."
