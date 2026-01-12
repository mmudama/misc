package local.pipeline;

// Flink core imports - these handle the streaming execution environment
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Kafka connector imports - these integrate Flink with Kafka
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

// Avro imports - for decoding the Confluent Avro wire format
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

// JSON processing - Jackson is used to parse/generate JSON
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

// Java standard library
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flink streaming job that reads procstat snapshots from Kafka,
 * calculates CPU utilization deltas between consecutive snapshots,
 * and publishes enriched metrics to a downstream Kafka topic.
 * 
 * Flow: Kafka (Avro) -> Decode -> Calculate Deltas -> Kafka (JSON)
 */
public class CpuEnrichmentConsumer {
    
    // Configuration from environment variables with sensible defaults
    private static final String REGISTRY_URL = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092");
    private static final String INPUT_TOPIC = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC", "procstat_snapshots");
    private static final String OUTPUT_TOPIC = System.getenv().getOrDefault("KAFKA_OUTPUT_TOPIC", "procstat_metrics");
    private static final String CONSUMER_GROUP_ID = System.getenv().getOrDefault("KAFKA_GROUP_ID", "cpu-enrichment-consumer");

    /**
     * Application entry point. Sets up the Flink streaming pipeline.
     */
    public static void main(String[] args) throws Exception {
        // Create the Flink execution environment - this is the context for all streaming operations
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Remove parallelism setting - now that we're using keyBy(), Flink can parallelize
        // Each hostname key will be processed by a single worker, maintaining state correctly
        
        // Configure Kafka source to read raw Avro bytes
        // We use a custom deserializer (AvroToJsonDeserializationSchema) that:
        // 1. Reads the Confluent Avro wire format (magic byte + schema ID + Avro data)
        // 2. Fetches the schema from Schema Registry
        // 3. Decodes the Avro binary to a JSON string
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                // Start from earliest to process all messages (for development/testing)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // Use our custom Avro deserializer
                .setValueOnlyDeserializer(new AvroToJsonDeserializationSchema(REGISTRY_URL, INPUT_TOPIC))
                .build();
        
        // Create a DataStream from the Kafka source
        // WatermarkStrategy.noWatermarks() means we're using processing-time semantics
        // (no event-time windowing or late data handling)
        DataStream<String> rawStream = env.fromSource(
            source, 
            WatermarkStrategy.noWatermarks(), 
            "Kafka Avro Source"
        );
        
        // Key by hostname to maintain separate state per host
        // ValueState requires a keyed stream - each key (hostname) gets its own state
        // This allows multiple hosts to send data simultaneously
        DataStream<String> keyedStream = rawStream.keyBy(json -> {
            try {
                JsonNode node = new ObjectMapper().readTree(json);
                return node.get("hostname").asText();
            } catch (Exception e) {
                return "unknown";
            }
        });
        
        // Apply the CPU delta calculation function
        // This is a stateful operation that remembers the previous snapshot per hostname
        DataStream<String> enrichedStream = keyedStream
                .map(new CpuDeltaCalculator())
                .filter(json -> json != null); // Remove nulls (first record has no previous state)
        
        // Configure Kafka sink to write enriched JSON metrics
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        
        // Attach the sink to the enriched stream
        enrichedStream.sinkTo(sink);
        
        // Execute the Flink job (this blocks until the job is cancelled)
        env.execute("CPU Enrichment Consumer");
    }
    
    /**
     * Stateful map function that calculates CPU utilization deltas.
     * 
     * Flink's RichMapFunction provides access to:
     * - open(): initialization method called once per parallel instance
     * - State API: managed state that survives failures via checkpoints
     * - RuntimeContext: access to metrics, accumulators, etc.
     */
    public static class CpuDeltaCalculator extends RichMapFunction<String, String> {
        
        // Jackson ObjectMapper for JSON parsing/generation (thread-safe, reuse across records)
        private transient ObjectMapper mapper;
        
        // Flink managed state - stores the previous snapshot for delta calculation
        // ValueState works on keyed streams - each key (hostname) maintains its own state
        // Flink checkpoints this state automatically for fault tolerance
        private transient ValueState<JsonNode> previousSnapshotState;
        
        /**
         * Initialization method called once when the function is first created.
         * Use this to set up non-serializable fields (marked 'transient').
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize the JSON mapper
            mapper = new ObjectMapper();
            
            // Register the state descriptor with Flink
            // This tells Flink how to serialize/deserialize the state for checkpointing
            ValueStateDescriptor<JsonNode> descriptor = new ValueStateDescriptor<>(
                "previousSnapshot",  // State name (for debugging/monitoring)
                JsonNode.class       // Type information for serialization
            );
            previousSnapshotState = getRuntimeContext().getState(descriptor);
        }
        
        /**
         * Process each incoming record (a JSON snapshot) and calculate CPU deltas.
         * 
         * @param jsonSnapshot JSON string of the current /proc/stat snapshot
         * @return JSON string with CPU utilization metrics, or null for the first record
         */
        @Override
        public String map(String jsonSnapshot) throws Exception {
            // Parse the incoming JSON snapshot
            JsonNode currentSnapshot = mapper.readTree(jsonSnapshot);
            
            // Retrieve the previous snapshot from Flink state (null on first record)
            JsonNode previousSnapshot = previousSnapshotState.value();
            
            // First record - no previous state to compare against
            if (previousSnapshot == null) {
                // Store this snapshot for the next iteration
                previousSnapshotState.update(currentSnapshot);
                return null; // Filter will remove this
            }
            
            // Calculate deltas and build enriched output
            ObjectNode enriched = mapper.createObjectNode();
            
            // Copy timestamp from current snapshot
            enriched.put("timestamp", currentSnapshot.get("timestamp").asLong());
            
            // Calculate time delta in milliseconds
            long timeDeltaMs = currentSnapshot.get("timestamp").asLong() 
                             - previousSnapshot.get("timestamp").asLong();
            enriched.put("time_delta_ms", timeDeltaMs);
            
            // Array to hold per-CPU metrics
            ArrayNode cpuMetrics = mapper.createArrayNode();
            
            // Iterate over each CPU in the current snapshot
            JsonNode currentCpus = currentSnapshot.get("cpus");
            JsonNode previousCpus = previousSnapshot.get("cpus");
            
            for (int i = 0; i < currentCpus.size(); i++) {
                JsonNode currCpu = currentCpus.get(i);
                
                // Find matching CPU in previous snapshot by ID
                JsonNode prevCpu = findCpuById(previousCpus, currCpu.get("id").asText());
                
                if (prevCpu == null) {
                    continue; // Skip if CPU wasn't in previous snapshot (shouldn't happen)
                }
                
                // Calculate deltas for each CPU counter
                // These are cumulative counters that monotonically increase
                long userDelta = currCpu.get("user").asLong() - prevCpu.get("user").asLong();
                long niceDelta = currCpu.get("nice").asLong() - prevCpu.get("nice").asLong();
                long systemDelta = currCpu.get("system").asLong() - prevCpu.get("system").asLong();
                long idleDelta = currCpu.get("idle").asLong() - prevCpu.get("idle").asLong();
                long iowaitDelta = currCpu.get("iowait").asLong() - prevCpu.get("iowait").asLong();
                long irqDelta = currCpu.get("irq").asLong() - prevCpu.get("irq").asLong();
                long softirqDelta = currCpu.get("softirq").asLong() - prevCpu.get("softirq").asLong();
                long stealDelta = currCpu.get("steal").asLong() - prevCpu.get("steal").asLong();
                
                // Total time spent (in jiffies - kernel time units, usually 1/100th of a second)
                long totalDelta = userDelta + niceDelta + systemDelta + idleDelta 
                                + iowaitDelta + irqDelta + softirqDelta + stealDelta;
                
                // Calculate percentages (avoid division by zero)
                if (totalDelta > 0) {
                    ObjectNode cpuMetric = mapper.createObjectNode();
                    cpuMetric.put("cpu_id", currCpu.get("id").asText());
                    
                    // User time: normal processes executing in user mode
                    cpuMetric.put("user_pct", (userDelta * 100.0) / totalDelta);
                    
                    // System time: processes executing in kernel mode
                    cpuMetric.put("system_pct", (systemDelta * 100.0) / totalDelta);
                    
                    // Idle time: CPU doing nothing
                    cpuMetric.put("idle_pct", (idleDelta * 100.0) / totalDelta);
                    
                    // IO wait: idle but waiting for I/O to complete
                    cpuMetric.put("iowait_pct", (iowaitDelta * 100.0) / totalDelta);
                    
                    // Total busy: everything except idle
                    long busyDelta = totalDelta - idleDelta;
                    cpuMetric.put("total_busy_pct", (busyDelta * 100.0) / totalDelta);
                    
                    cpuMetrics.add(cpuMetric);
                }
            }
            
            enriched.set("cpu_metrics", cpuMetrics);
            
            // Update state with current snapshot for next iteration
            previousSnapshotState.update(currentSnapshot);
            
            // Return enriched metrics as JSON string
            return mapper.writeValueAsString(enriched);
        }
        
        /**
         * Helper method to find a CPU by ID in an array of CPU objects.
         */
        private JsonNode findCpuById(JsonNode cpus, String cpuId) {
            for (JsonNode cpu : cpus) {
                if (cpu.get("id").asText().equals(cpuId)) {
                    return cpu;
                }
            }
            return null;
        }
    }
}
