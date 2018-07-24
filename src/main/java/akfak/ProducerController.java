package akfak;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, Integer> intTemplate;

    @Autowired
    private KafkaTemplate<String, Object> avroTemplate;

    @PostMapping("gen1")
    public void gen1(@RequestParam int count) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("user.avsc"));
        GenericRecord r = new GenericData.Record(schema);
        r.put("name", "me");
        r.put("number", count);

        avroTemplate.send("record", r);

        //IntStream.range(0, count)
        //         .forEach(i -> intTemplate.send("record", "" + i, count));
    }
}
