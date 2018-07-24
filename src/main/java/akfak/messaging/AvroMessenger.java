package akfak.messaging;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

@RestController
@RequestMapping(path = "avro")
public class AvroMessenger {

    private static final Logger logger = LoggerFactory.getLogger(AvroMessenger.class);

    private static final String topic = "avro";

    @Autowired
    private KafkaTemplate<String, Object> template;

    @PostMapping("gen")
    public void gen(@RequestParam int count) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("user.avsc"));
        GenericRecord r = new GenericData.Record(schema);
        r.put("name", "me");
        IntStream.range(0, count)
                 .forEach(i -> {
                     r.put("number", i);
                     template.send(topic, "" + i, r);
                 });
    }

    @KafkaListener(topics = topic, containerFactory = "avroListenerContainerFactory")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("CR: " + cr.key() + " " + cr.value());
    }
}
