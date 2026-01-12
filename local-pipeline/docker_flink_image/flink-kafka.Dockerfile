FROM flink:1.19.3-scala_2.12

# Install Kafka DataStream connector and Kafka clients into Flink classpath
RUN set -eux; \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar; \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

# Set jobmanager address to use docker-compose service name
RUN echo "jobmanager.rpc.address: jobmanager" >> /opt/flink/conf/flink-conf.yaml && \
    echo "jobmanager.memory.process.size: 1600m" >> /opt/flink/conf/flink-conf.yaml && \
    echo "taskmanager.numberOfTaskSlots: 2" >> /opt/flink/conf/flink-conf.yaml && \
    echo "taskmanager.memory.process.size: 1728m" >> /opt/flink/conf/flink-conf.yaml

# Copy custom entrypoint script for auto-submitting consumer job on JobManager startup
COPY entrypoint.sh /opt/flink/entrypoint.sh
RUN chmod +x /opt/flink/entrypoint.sh

# Java consumer JAR will be mounted via compose volumes
ENTRYPOINT ["/opt/flink/entrypoint.sh"]
