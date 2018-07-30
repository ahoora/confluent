package prozess.collaboration.specific;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import prozess.model.RequestedEntity;
import prozess.model.ValidatedEntity;
import prozess.model.ValidationResult;

@Component
@EnableBinding(StreamBindings.class)
public class SpecificValidationComponent {

    private static final Logger logger = LoggerFactory.getLogger(SpecificValidationComponent.class);

    @Autowired
    private SpecificAvroSerde<ValidatedEntity> serde;

    @StreamListener
    public void process(@Input("requestSpecific") KStream<String, RequestedEntity> requests) {
        requests.mapValues(this::validate)
                .peek((k, v) -> logger.info("validated (s): " + k + " " + v))
                .to("validateSpecific", Produced.valueSerde(serde));
    }

    private ValidatedEntity validate(final RequestedEntity requestedEntity) {
        ValidationResult result = "bad".equals(requestedEntity.getEntity().getName())
                ? ValidationResult.ERROR
                : ValidationResult.OK;

        return ValidatedEntity.newBuilder()
                       .setRequest(requestedEntity)
                       .setValidationResult(result)
                       .build();
    }
}
