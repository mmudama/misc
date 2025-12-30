package local.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink streaming job that reads enriched CPU metrics from Kafka
 * and writes them to Parquet files on the local filesystem.
 * 
 * Flow: Kafka (JSON) -> Parse -> Convert to Avro -> Parquet files
 * 
 * This consumer demonstrates a common pattern in data lakes:
 * - Stage 1: Stream processing (CpuEnrichmentConsumer) enriches raw data
 * - Stage 2: Data archival (this consumer) writes to long-term storage
 * - Stage 3+: Analytics tools (DuckDB, Spark, etc.) query Parquet files
 */
public class ParquetWriterConsumer {
    
    // Configuration from environment variables with sensible defaults
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092");
    private static final String INPUT_TOPIC = System.getenv().getOrDefault("KAFKA_OUTPUT_TOPIC", "procstat_metrics");
    private static final String CONSUMER_GROUP_ID = System.getenv().getOrDefault("PARQUET_CONSUMER_GROUP_ID", "parquet-writer-consumer");
    private static final String OUTPUT_PATH = System.getenv().getOrDefault("PARQUET_OUTPUT_PATH", "/opt/flink/output");

    // Simple POJOs for Avro reflection to avoid GenericRecord serialization issues
    public static class CpuMetric {
        public String cpu_id;
        public double user_pct;
        public double system_pct;
        public double idle_pct;
        public double iowait_pct;
        public double total_busy_pct;

        public CpuMetric() {}
    }

    public static class CpuMetricsRecord {
        public long timestamp;
        public long time_delta_ms;
        public List<CpuMetric> cpu_metrics;

        public CpuMetricsRecord() {}
    }

    public static void main(String[] args) throws Exception {
        // Create the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for exactly-once semantics with Parquet files
        // Checkpoint every 60 seconds - this is when Parquet files are finalized
        env.enableCheckpointing(60000);
        
        // Configure Kafka source to read enriched JSON metrics
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create a DataStream from the Kafka source
        DataStream<String> jsonStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka JSON Source"
        );
        
        final ObjectMapper mapper = new ObjectMapper();

        DataStream<CpuMetricsRecord> recordStream = jsonStream.map(json -> {
            JsonNode root = mapper.readTree(json);

            CpuMetricsRecord rec = new CpuMetricsRecord();
            rec.timestamp = root.get("timestamp").asLong();
            rec.time_delta_ms = root.get("time_delta_ms").asLong();

            List<CpuMetric> list = new ArrayList<>();
            for (JsonNode cpuNode : root.get("cpu_metrics")) {
                CpuMetric m = new CpuMetric();
                m.cpu_id = cpuNode.get("cpu_id").asText();
                m.user_pct = cpuNode.get("user_pct").asDouble();
                m.system_pct = cpuNode.get("system_pct").asDouble();
                m.idle_pct = cpuNode.get("idle_pct").asDouble();
                m.iowait_pct = cpuNode.get("iowait_pct").asDouble();
                m.total_busy_pct = cpuNode.get("total_busy_pct").asDouble();
                list.add(m);
            }
            rec.cpu_metrics = list;
            return rec;
        }).returns(TypeInformation.of(CpuMetricsRecord.class));

        final StreamingFileSink<CpuMetricsRecord> sink = StreamingFileSink
            .forBulkFormat(
                new Path(OUTPUT_PATH),
                AvroParquetWriters.forReflectRecord(CpuMetricsRecord.class)
            )
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();

        recordStream.addSink(sink);
        
        // Execute the Flink job
        env.execute("Parquet Writer Consumer");
    }
}
