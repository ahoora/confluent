package prozess.collaboration.generic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.util.UUID;

@RestController
@RequestMapping("/collab/generic")
@EnableBinding(StreamBindings.class)
@Import(MessengerConfig.class)
public class CollaborationController {

    private static final Logger logger = LoggerFactory.getLogger(CollaborationController.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private Schema entityRequestSchema;

    @Autowired
    private Schema entitySchema;

    private Flux<KeyValue<UUID, GenericRecord>> completedRequests;

    @StreamListener
    public void process(@Input("validate") KStream<String, GenericRecord> validations) {
        EmitterProcessor emitter = EmitterProcessor.<KeyValue<UUID, String>>create();
        FluxSink<KeyValue<UUID, GenericRecord>> sink = emitter.sink();
        completedRequests = emitter.publish().autoConnect();

        validations.peek((k, v) -> {
            logger.info("completed (g): " + k + " " + v);
            sink.next(KeyValue.pair(UUID.fromString(k), v));
        });
    }

    @PostMapping("/entity")
    public Mono<String> createEntity(@Valid @RequestBody CreateEntityRequest request) {
        UUID uuid = UUID.randomUUID();

        Mono<String> ret = completedRequests.filter(u -> uuid.equals(u.key))
                                            .map(kv -> {
                                                GenericEnumSymbol result = (GenericEnumSymbol) kv.value.get("validationResult");
                                                if (!"OK".equals(result.toString())) {
                                                    throw new RuntimeException("Validation error");
                                                }
                                                return kv;
                                            })
                                            .map(kv -> kv.toString())
                                            .next();

        logger.info(entityRequestSchema.toString(true));
        GenericRecord entityRequestRecord = new GenericData.Record(entityRequestSchema);
        GenericRecord entity = new GenericData.Record(entitySchema);
        entity.put("name", request.name);
        entityRequestRecord.put("entity", entity);
        entityRequestRecord.put("requestor", "someone");
        kafkaTemplate.send("request", uuid.toString(), entityRequestRecord);
        return ret;
    }
}
