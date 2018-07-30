package prozess.collaboration.specific;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;

public interface StreamBindings {
    @Input("requestSpecific")
    KStream<?, ?> requestSpecific();

    @Input("validateSpecific")
    KStream<?, ?> validateSpecific();
}
