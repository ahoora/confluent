package prozess.collaboration;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
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
@EnableBinding(StreamBindings.class)
@Import(KafkaConfiguration.class)
public class CollaborationController {

    private static final Logger logger = LoggerFactory.getLogger(CollaborationController.class);

    private static final String ENTITY_STORE = "entity-store";

    @Autowired
    private QueryableStoreRegistry queryableStoreRegistry;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private Flux<KeyValue<UUID, ValidatedEntity>> completedRequests;

    @StreamListener
    public void process(@Input("validate") KStream<String, ValidatedEntity> validations) {
        EmitterProcessor emitter = EmitterProcessor.<KeyValue<UUID, ValidatedEntity>>create();
        FluxSink<KeyValue<UUID, ValidatedEntity>> sink = emitter.sink();
        completedRequests = emitter.publish().autoConnect();

        validations.peek((k, v) -> {
                        logger.info("completed (s): " + k + " " + v);
                        sink.next(KeyValue.pair(UUID.fromString(k), v));
                    })
                   .groupByKey()
                   .reduce((oldValue, newValue) -> newValue, Materialized.<String, ValidatedEntity, KeyValueStore<Bytes, byte[]>>as(ENTITY_STORE));
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

        kafkaTemplate.send("request", uuid.toString(), requestedEntity);
        return ret;
    }

    @GetMapping("/entity")
    public Flux<ValidatedEntity> entities() {
        ReadOnlyKeyValueStore<String, ValidatedEntity> store = queryableStoreRegistry.getQueryableStoreType(ENTITY_STORE, QueryableStoreTypes.<String,ValidatedEntity>keyValueStore());
        Iterable<KeyValue<String, ValidatedEntity>> iterable = () -> store.all();
        return Flux.fromIterable(iterable)
                   .map(kv -> kv.value);
    }
}
