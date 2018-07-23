package akfak;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.IntStream;

@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, Integer> intTemplate;

    @PostMapping("gen1")
    public void gen1(@RequestParam int count) {
        IntStream.range(0, count)
                 .forEach(i -> intTemplate.send("record", "" + i, count));
    }
}
