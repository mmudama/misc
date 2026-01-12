FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies for Confluent Schema Registry client and Avro
RUN pip install --no-cache-dir \
    confluent-kafka \
    fastavro \
    attrs \
    certifi \
    httpx \
    cachetools \
    authlib

# Copy producer script and Avro schema into the image
COPY bin/producer_procstat_avro_container.py /app/bin/producer_procstat_avro_container.py
COPY bin/procstat_schema.avsc /app/bin/procstat_schema.avsc

# Default environment; can be overridden via docker-compose
ENV SLEEP_SECONDS=60 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:19092 \
    SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    TOPIC=procstat_snapshots

# Run the producer
CMD ["python", "-u", "/app/bin/producer_procstat_avro_container.py"]
