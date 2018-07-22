package akfak;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class StreamBuilderComponent {

//    public void init() {
//        Properties props = new Properties();
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        StreamsBuilder builder = new StreamsBuilder();
//        builder.stream(Serdes.String(), Serdes.String(), "akfak")
//               .
//    }

}
