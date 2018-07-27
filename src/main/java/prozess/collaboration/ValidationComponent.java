package prozess.collaboration;

import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@EnableBinding(StreamBindings.class)
public class ValidationComponent {

    private static final Logger logger = LoggerFactory.getLogger(ValidationComponent.class);

    @StreamListener
    public void process(@Input("request") KStream<String, String> requests) {
        requests.mapValues(v -> "bad".equals(v) ? "error" : "ok")
                .peek((k, v) -> logger.info("validated: " + k + " " + v))
                .to("validate");
    }

}
