package prozess.collaboration.generic;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@EnableBinding(StreamBindings.class)
public class ValidationComponent {

    private static final Logger logger = LoggerFactory.getLogger(ValidationComponent.class);

    @Autowired
    private Schema entityValidationSchema;

    @Autowired
    private GenericAvroSerde avroSerde;

    @StreamListener
    public void process(@Input("request") KStream<String, GenericRecord> requests) {
        requests.mapValues(this::validatedEntity)
                .mapValues(this::validate)
                .peek((k, v) -> logger.info("validated (g): " + k + " " + v))
                .to("validate", Produced.valueSerde(avroSerde));
    }

    private GenericRecord validatedEntity(final GenericRecord requestedEntity) {
        GenericRecord validatedEntity = new GenericData.Record(entityValidationSchema);
        validatedEntity.put("request", requestedEntity);
        return validatedEntity;
    }

    private GenericRecord validate(final GenericRecord validatedEntity) {
        GenericRecord request = (GenericRecord) validatedEntity.get("request");
        GenericRecord entity = (GenericRecord) request.get("entity");
        GenericEnumSymbol result = new GenericData.EnumSymbol(
                entityValidationSchema.getField("validationResult").schema(),
                "bad".equals(entity.get("name").toString())
                        ? "ERROR"
                        : "OK");
        validatedEntity.put("validationResult", result);
        return validatedEntity;
    }
}
