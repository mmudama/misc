package local.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProcstatConsumer {
    private static final String REGISTRY_URL = "http://schema-registry:8081";

    /**
     * Main entry point for the ProcStat consumer Flink job.
     * Sets up a Kafka source to read from the procstat_snapshots topic,
     * decodes Confluent Avro-formatted messages, and prints the results.
     *
     * @param args command-line arguments (not used)
     * @throws Exception if job execution fails
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers("kafka:19092")
                .setTopics("procstat_snapshots")
                .setGroupId("flink-java-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new ByteArrayValueDeserializationSchema()))
                .build();

        DataStreamSource<byte[]> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        stream.map(ProcstatConsumer::decodeConfluentAvro).print();

        env.execute("ProcStat Java Consumer");
    }

    private static final HttpClient http = HttpClient.newHttpClient();
    private static final Map<Integer, Schema> schemaCache = new ConcurrentHashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Custom deserialization schema that passes through raw byte arrays.
     * Used by KafkaRecordDeserializationSchema to extract message values without modification.
     */
    private static class ByteArrayValueDeserializationSchema implements DeserializationSchema<byte[]> {
        /**
         * Deserializes a Kafka message by returning the raw bytes as-is.
         *
         * @param message the raw byte array from Kafka
         * @return the same byte array without modification
         */
        @Override
        public byte[] deserialize(byte[] message) {
            return message;
        }

        /**
         * Indicates whether the stream has ended.
         *
         * @param nextElement the next element in the stream
         * @return false, as the stream is continuous
         */
        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        /**
         * Provides the type information for the deserialized output.
         *
         * @return TypeInformation for byte array
         */
        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(byte[].class);
        }
    }

    /**
     * Decodes a Confluent Avro-formatted message.
     * Extracts the magic byte and schema ID from the message header,
     * fetches the corresponding schema from the Schema Registry,
     * and deserializes the Avro payload using GenericDatumReader.
     *
     * @param value the raw Kafka message bytes (Confluent Avro wire format)
     * @return a string representation of the decoded GenericRecord, or "{}" if decoding fails
     * @throws Exception if deserialization fails
     */
    private static String decodeConfluentAvro(byte[] value) throws Exception {
        if (value == null || value.length < 5) {
            return "{}";
        }

        ByteBuffer buffer = ByteBuffer.wrap(value);
        byte magic = buffer.get();
        if (magic != 0) {
            return "{}";
        }
        int schemaId = buffer.getInt();

        Schema schema = schemaCache.computeIfAbsent(schemaId, ProcstatConsumer::fetchSchema);
        if (schema == null) {
            return "{}";
        }

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, 5, value.length - 5, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord record = reader.read(null, decoder);

        return record.toString();
    }

    /**
     * Fetches an Avro schema from the Schema Registry by ID.
     * Sends an HTTP GET request to the registry endpoint and caches the result.
     * Results are cached in schemaCache to avoid repeated lookups for the same schema ID.
     *
     * @param id the schema ID to fetch
     * @return the parsed Avro Schema, or null if the fetch or parsing fails
     */
    private static Schema fetchSchema(int id) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(REGISTRY_URL + "/schemas/ids/" + id))
                    .GET()
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 == 2) {
                JsonNode root = mapper.readTree(resp.body());
                JsonNode schemaNode = root.get("schema");
                if (schemaNode != null && schemaNode.isTextual()) {
                    String schemaJson = schemaNode.asText();
                    return new Schema.Parser().parse(schemaJson);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }
}
