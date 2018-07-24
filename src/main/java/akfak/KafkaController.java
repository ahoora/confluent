package akfak;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RestController
@EnableBinding(KafkaController.KafkaStreamsProcessorX.class)
public class KafkaController {

    public static Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    private QueryableStoreRegistry queryableStoreRegistry;

//    @StreamListener("input")
//    public void process(KStream<String, String> input) {
//        input.groupByKey()
//             .reduce((o, n) -> n, Materialized.as("all-data"));
//    }

    @StreamListener
    public void processInts(@Input("intStream") KStream<String, Integer> intStream,
                            @Input("stringStream") KStream<String, String> stringStream,
                            @Input("avroStream") KStream<String, Object> avroStream) {
        intStream.peek((k, v) -> logger.info("int: " + k + " " + v));
        stringStream.peek((k, v) -> logger.info("input: " + k + " " + v));
        avroStream.peek((k, v) -> logger.info("avro: " + k + " " + v));
    }

    @GetMapping("/table")
    public String table(){
        ReadOnlyKeyValueStore<String, String> store = queryableStoreRegistry.getQueryableStoreType("all-data", QueryableStoreTypes.<String,String>keyValueStore());

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(store.all(), Spliterator.ORDERED), false)
                            .map(kv -> kv.key + ": " + kv.value)
                            .collect(Collectors.joining("\n"));
    }

    interface KafkaStreamsProcessorX {
        @Input("stringStream")
        KStream<?, ?> stringStream();

        @Input("intStream")
        KStream<?, ?> intStream();

        @Input("avroStream")
        KStream<?, ?> avroStream();
    }

}
