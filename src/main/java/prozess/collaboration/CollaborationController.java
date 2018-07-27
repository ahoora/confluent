package prozess.collaboration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.util.UUID;

@RestController
@RequestMapping("/collab")
@EnableBinding(StreamBindings.class)
public class CollaborationController {

    private static final Logger logger = LoggerFactory.getLogger(CollaborationController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Flux<KeyValue<UUID, String>> completedRequests;

    @StreamListener
    public void process(@Input("validate") KStream<String, String> validations) {
        EmitterProcessor emitter = EmitterProcessor.<KeyValue<UUID, String>>create();
        FluxSink<KeyValue<UUID, String>> sink = emitter.sink();
        completedRequests = emitter.publish().autoConnect();

        validations.peek((k, v) -> {
            logger.info("completed: " + k + " " + v);
            sink.next(KeyValue.pair(UUID.fromString(k), v));
        });
    }

    @PostMapping("/request")
    public Mono<String> request(@Valid @RequestBody Request request) {
        UUID uuid = UUID.randomUUID();

        Mono<String> ret = completedRequests.filter(u -> uuid.equals(uuid))
                                            .map(kv -> {
                                                if (!kv.value.equals("ok")) {
                                                    throw new RuntimeException("Validation error");
                                                }
                                                return kv;
                                            })
                                            .map(kv -> kv.key.toString())
                                            .next();
        kafkaTemplate.send("request", uuid.toString(), request.message);
        return ret;
    }
}
