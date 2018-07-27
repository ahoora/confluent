package prozess.collaboration;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;

public interface StreamBindings {
    @Input("request")
    KStream<?, ?> request();

    @Input("validate")
    KStream<?, ?> validate();
}
