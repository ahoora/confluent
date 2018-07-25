package prozess.messaging;

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

import java.time.Instant;
import java.util.stream.IntStream;

@RestController
@RequestMapping(path = "string")
public class StringMessenger {

    private static final Logger logger = LoggerFactory.getLogger(StringMessenger.class);

    private static final String topic = "string";

    @Autowired
    private KafkaTemplate<String, String> template;

    @PostMapping("gen")
    public void gen(@RequestParam int count) {
        IntStream.range(0, count)
                 .forEach(i -> template.send(topic, "" + i, Instant.now().toString()));
    }

    @KafkaListener(topics = topic, containerFactory = "stringListenerContainerFactory")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("CR: " + cr.key() + " " + cr.value());
    }
}
