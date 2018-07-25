package prozess;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
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

    @Value("${spring.cloud.stream.kafka.streams.binder.configuration.schema.registry.url}")
    private String schemaRegistryUrl;

    private GenericAvroSerde avroSerde() {
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        GenericAvroSerde avroSerde = new GenericAvroSerde();
        avroSerde.configure(serdeConfig, false);
        return avroSerde;
    }

    private String textDump(ReadOnlyKeyValueStore<?, ?> store) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(store.all(), Spliterator.ORDERED), false)
                            .map(kv -> kv.key + ": " + kv.value)
                            .collect(Collectors.joining("\n"));
    }

    @StreamListener
    public void processInts(@Input("intStream") KStream<String, Integer> intStream,
                            @Input("stringStream") KStream<String, String> stringStream,
                            @Input("avroStream") KStream<String, GenericRecord> avroStream) {
        intStream.peek((k, v) -> logger.info("int: " + k + " " + v))
                 .groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()))
                 .reduce((oldValue, newValue) -> newValue,
                         Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("int-t")
                                     .withValueSerde(Serdes.Integer()));

        stringStream.peek((k, v) -> logger.info("string: " + k + " " + v))
                    .groupByKey()
                    .reduce((oldValue, newValue) -> newValue, Materialized.as("string-t"));

        final GenericAvroSerde serde = avroSerde();
        avroStream.peek((k, v) -> logger.info("avro: " + k + " " + v + " " + v.getClass().getName()))
                  .groupByKey(Serialized.with(Serdes.String(), serde))
                  .reduce((oldValue, newValue) -> newValue,
                          Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as("avro-t")
                                      .withValueSerde(serde));
    }

    @GetMapping("/string/table")
    public String stringTable() {
        ReadOnlyKeyValueStore<String, String> store = queryableStoreRegistry.getQueryableStoreType("string-t", QueryableStoreTypes.<String,String>keyValueStore());
        return textDump(store);
    }

    @GetMapping("/int/table")
    public String intTable() {
        ReadOnlyKeyValueStore<String, Integer> store = queryableStoreRegistry.getQueryableStoreType("int-t", QueryableStoreTypes.<String,Integer>keyValueStore());
        return textDump(store);
    }

    @GetMapping("/avro/table")
    public String avroTable() {
        ReadOnlyKeyValueStore<String, GenericRecord> store = queryableStoreRegistry.getQueryableStoreType("avro-t", QueryableStoreTypes.keyValueStore());
        return textDump(store);
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
