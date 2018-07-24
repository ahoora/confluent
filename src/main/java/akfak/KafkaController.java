package akfak;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@RestController
@EnableBinding(KafkaController.KafkaStreamsProcessorX.class)
public class KafkaController {

    public static Logger logger = LoggerFactory.getLogger(KafkaController.class);

    public int counter = 0;

    @Autowired
    private KafkaTemplate<String, String> stringTemplate;

    @Autowired
    private QueryableStoreRegistry queryableStoreRegistry;

    @StreamListener("input")
    public void process(KStream<String, String> input) {
        input.groupByKey()
             .reduce((o, n) -> n, Materialized.as("all-data"));
    }

    @GetMapping("/table")
    public String table(){
        ReadOnlyKeyValueStore<String, String> store = queryableStoreRegistry.getQueryableStoreType("all-data", QueryableStoreTypes.<String,String>keyValueStore());

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(store.all(), Spliterator.ORDERED), false)
                            .map(kv -> kv.key + ": " + kv.value)
                            .collect(Collectors.joining("\n"));
    }

    @PostMapping("gen")
    public void gen(@RequestParam int count) {
        IntStream.range(0, count)
                 .forEach(i -> stringTemplate.send("akfak", "" + i, Instant.now().toString()));
    }

    @KafkaListener(topics = "record", containerFactory = "avroListenerContainerFactory")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("CR: " + cr.key() + " " + cr.value());
    }

    interface KafkaStreamsProcessorX {
        @Input("input")
        KStream<?, ?> input();
    }

}
