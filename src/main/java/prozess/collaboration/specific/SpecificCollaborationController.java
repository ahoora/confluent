package prozess.collaboration.specific;

import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import prozess.collaboration.CreateEntityRequest;
import prozess.messaging.MessengerConfig;
import prozess.model.Entity;
import prozess.model.RequestedEntity;
import prozess.model.ValidatedEntity;
import prozess.model.ValidationResult;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.util.UUID;

@RestController
@RequestMapping("/collab/specific")
@EnableBinding(StreamBindings.class)
@Import(KafkaConfiguration.class)
public class SpecificCollaborationController {

    private static final Logger logger = LoggerFactory.getLogger(SpecificCollaborationController.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private Flux<KeyValue<UUID, ValidatedEntity>> completedRequests;

    @StreamListener
    public void process(@Input("validateSpecific") KStream<String, ValidatedEntity> validations) {
        EmitterProcessor emitter = EmitterProcessor.<KeyValue<UUID, ValidatedEntity>>create();
        FluxSink<KeyValue<UUID, ValidatedEntity>> sink = emitter.sink();
        completedRequests = emitter.publish().autoConnect();

        validations.peek((k, v) -> {
            logger.info("completed (s): " + k + " " + v);
            sink.next(KeyValue.pair(UUID.fromString(k), v));
        });
    }

    @PostMapping("/entity")
    public Mono<String> createEntity(@Valid @RequestBody CreateEntityRequest request) {
        UUID uuid = UUID.randomUUID();

        Mono<String> ret = completedRequests.filter(u -> uuid.equals(u.key))
                                            .map(kv -> {
                                                if (ValidationResult.ERROR == kv.value.getValidationResult()) {
                                                    throw new RuntimeException("Validation error");
                                                }
                                                return kv;
                                            })
                                            .map(kv -> kv.toString())
                                            .next();

        Entity entity = Entity.newBuilder()
                              .setName(request.name)
                              .build();
        RequestedEntity requestedEntity = RequestedEntity.newBuilder()
                                                         .setEntity(entity)
                                                         .setRequestor("someone")
                                                         .build();

        kafkaTemplate.send("requestSpecific", uuid.toString(), requestedEntity);
        return ret;
    }
}
