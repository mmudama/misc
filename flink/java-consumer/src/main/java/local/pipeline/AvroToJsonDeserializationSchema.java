package local.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom Flink deserialization schema that converts Confluent Avro messages to JSON strings.
 * 
 * Confluent Avro Wire Format:
 * - Byte 0: Magic byte (always 0x0)
 * - Bytes 1-4: Schema ID (4-byte big-endian integer)
 * - Bytes 5+: Avro-encoded payload
 * 
 * This deserializer:
 * 1. Parses the Confluent wire format manually
 * 2. Fetches schemas from Schema Registry via HTTP (with caching)
 * 3. Deserializes Avro binary to GenericRecord using Avro's GenericDatumReader
 * 4. Converts GenericRecord to a Map and then to JSON string
 * 
 * Why manual parsing instead of KafkaAvroDeserializer?
 * - KafkaAvroDeserializer isn't Serializable, which breaks Flink's distribution
 * - Manual approach gives us full control and works reliably with Flink
 * - HTTP client and Avro readers are all Serializable or can be recreated
 */
public class AvroToJsonDeserializationSchema implements DeserializationSchema<String> {
    
    // Schema Registry configuration
    private final String schemaRegistryUrl;
    
    // Schema cache (shared across all records, thread-safe)
    // Maps schema ID -> parsed Avro Schema
    private static final Map<Integer, Schema> SCHEMA_CACHE = new ConcurrentHashMap<>();
    
    // HTTP client for fetching schemas (transient = recreated after deserialization)
    private static transient HttpClient httpClient;
    
    // Jackson mapper for JSON conversion (transient = recreated after deserialization)
    private transient ObjectMapper mapper;

    /**
     * Constructor that saves Schema Registry URL.
     * 
     * @param schemaRegistryUrl URL of the Confluent Schema Registry
     * @param topic Kafka topic name (not used in manual approach, but kept for compatibility)
     */
    public AvroToJsonDeserializationSchema(String schemaRegistryUrl, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }
    
    /**
     * Ensure transient fields are initialized (lazy initialization).
     * Called before first use of httpClient or mapper.
     */
    private void ensureInitialized() {
        if (httpClient == null) {
            httpClient = HttpClient.newHttpClient();
        }
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
    }

    /**
     * Deserialize a Confluent Avro message to a JSON string.
     * 
     * This is called by Flink for each Kafka message value.
     * 
     * @param message Raw bytes from Kafka (Confluent Avro wire format)
     * @return JSON string representation of the Avro record
     */
    @Override
    public String deserialize(byte[] message) throws IOException {
        ensureInitialized();
        
        // Validate message has minimum length (magic byte + schema ID)
        if (message == null || message.length < 5) {
            return "{}";
        }
        
        // Parse Confluent wire format
        ByteBuffer buffer = ByteBuffer.wrap(message);
        
        // Byte 0: Magic byte (should be 0 for Confluent format)
        byte magic = buffer.get();
        if (magic != 0) {
            return "{}"; // Not Confluent format
        }
        
        // Bytes 1-4: Schema ID (big-endian 32-bit integer)
        int schemaId = buffer.getInt();
        
        // Remaining bytes: Avro-encoded payload
        byte[] avroData = new byte[message.length - 5];
        buffer.get(avroData);
        
        // Fetch schema from cache or Schema Registry
        Schema schema = SCHEMA_CACHE.computeIfAbsent(schemaId, this::fetchSchema);
        
        if (schema == null) {
            return "{}"; // Failed to fetch schema
        }
        
        try {
            // Decode Avro binary using GenericDatumReader
            // This is Avro's standard deserialization approach
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            GenericRecord record = reader.read(null, decoder);
            
            // Convert GenericRecord to JSON via Map
            return mapper.writeValueAsString(toMap(record));
            
        } catch (Exception e) {
            // Log error and return empty JSON
            System.err.println("Error decoding Avro: " + e.getMessage());
            return "{}";
        }
    }
    
    /**
     * Fetch an Avro schema from the Schema Registry by ID.
     * 
     * Makes an HTTP GET request to /schemas/ids/{id}
     * Response format: {"schema": "<escaped JSON schema string>"}
     * 
     * @param schemaId Schema ID from Confluent wire format
     * @return Parsed Avro Schema, or null if fetch fails
     */
    private Schema fetchSchema(int schemaId) {
        try {
            // Build HTTP request to Schema Registry
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(schemaRegistryUrl + "/schemas/ids/" + schemaId))
                    .GET()
                    .build();
            
            // Send request and get response
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            // Check for successful response (2xx status codes)
            if (response.statusCode() / 100 == 2) {
                // Parse JSON response to extract schema string
                JsonNode root = mapper.readTree(response.body());
                JsonNode schemaNode = root.get("schema");
                
                if (schemaNode != null && schemaNode.isTextual()) {
                    String schemaJson = schemaNode.asText();
                    // Parse the schema JSON string into an Avro Schema object
                    return new Schema.Parser().parse(schemaJson);
                }
            }
        } catch (Exception e) {
            System.err.println("Error fetching schema " + schemaId + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Convert an Avro GenericRecord to a Java Map recursively.
     * 
     * GenericRecord is Avro's generic in-memory representation.
     * It's like a Map but typed according to the Avro schema.
     * 
     * This handles nested records and arrays properly by recursing.
     * 
     * @param record Avro GenericRecord
     * @return Map<String, Object> with field names as keys
     */
    private Map<String, Object> toMap(GenericRecord record) {
        Map<String, Object> out = new HashMap<>();
        Schema schema = record.getSchema();
        
        // Iterate over all fields defined in the schema
        for (Schema.Field f : schema.getFields()) {
            Object v = record.get(f.name());
            out.put(f.name(), convertValue(v));
        }
        return out;
    }
    
    /**
     * Recursively convert Avro values to plain Java objects suitable for JSON serialization.
     * 
     * @param value Avro value (may be GenericRecord, array, primitive, etc.)
     * @return Plain Java object (Map, List, String, Number, etc.)
     */
    private Object convertValue(Object value) {
        if (value instanceof GenericRecord) {
            // Recursively convert nested records
            return toMap((GenericRecord) value);
        } else if (value instanceof java.util.List) {
            // Convert array/list elements recursively
            java.util.List<?> list = (java.util.List<?>) value;
            java.util.List<Object> converted = new java.util.ArrayList<>();
            for (Object item : list) {
                converted.add(convertValue(item));
            }
            return converted;
        } else {
            // Primitives (String, Number, Boolean, etc.) return as-is
            return value;
        }
    }

    /**
     * Signals whether the stream has ended.
     * For Kafka, streams are unbounded, so this always returns false.
     */
    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    /**
     * Tells Flink the type of objects this deserializer produces.
     * Used by Flink's type system for serialization and optimization.
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}

