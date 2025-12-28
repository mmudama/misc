FROM flink:1.19.3-scala_2.12

# Install Kafka DataStream connector and Kafka clients into Flink classpath
RUN set -eux; \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar; \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

# Copy custom entrypoint script for auto-submitting consumer job on JobManager startup
COPY entrypoint.sh /opt/flink/entrypoint.sh
RUN chmod +x /opt/flink/entrypoint.sh

# Keep Java consumer jar mounted via compose volumes
ENTRYPOINT ["/opt/flink/entrypoint.sh"]
