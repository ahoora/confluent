package prozess.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.stream.IntStream;

@RestController()
@RequestMapping(path = "int")
@Profile("kafka")
public class IntMessenger {

    private static final Logger logger = LoggerFactory.getLogger(IntMessenger.class);

    private static final String topic = "int";

    @Autowired
    private KafkaTemplate<String, Integer> template;

    @PostMapping("gen")
    public void gen(@Valid @RequestBody GenerateRequest request) {
        IntStream.range(0, request.count)
                 .forEach(i -> template.send(topic, "" + i, request.count));
    }

    @KafkaListener(topics = topic, containerFactory = "intListenerContainerFactory")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("CR: " + cr.key() + " " + cr.value());
    }
}
