package local.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AvroToJsonDeserializationSchema implements DeserializationSchema<String> {
    private final KafkaAvroDeserializer deserializer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String topic;

    public AvroToJsonDeserializationSchema(String schemaRegistryUrl, String topic) {
        this.topic = topic;
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", false);
        this.deserializer = new KafkaAvroDeserializer();
        this.deserializer.configure(props, false);
    }

    @Override
    public String deserialize(byte[] message) throws IOException {
        Object value = deserializer.deserialize(topic, message);
        if (value instanceof GenericRecord) {
            return mapper.writeValueAsString(toMap((GenericRecord) value));
        }
        // Primitives or other types
        return mapper.writeValueAsString(value);
    }

    private Map<String, Object> toMap(GenericRecord record) {
        Map<String, Object> out = new HashMap<>();
        Schema schema = record.getSchema();
        for (Schema.Field f : schema.getFields()) {
            Object v = record.get(f.name());
            out.put(f.name(), v);
        }
        return out;
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
