package prozess.collaboration;

import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Service;
import prozess.model.RequestedEntity;
import prozess.model.ValidatedEntity;
import prozess.model.ValidationResult;

@Service
@EnableBinding(StreamBindings.class)
public class ValidationService {

    private static final Logger logger = LoggerFactory.getLogger(ValidationService.class);

    @StreamListener
    public void process(@Input("request") KStream<String, RequestedEntity> requests) {
        requests.mapValues(this::validate)
                .peek((k, v) -> logger.info("validated (s): " + k + " " + v))
                .to("validate");
    }

    private ValidatedEntity validate(final RequestedEntity requestedEntity) {
        ValidationResult result = "bad".equals(requestedEntity.getEntity().getName())
                ? ValidationResult.ERROR
                : ValidationResult.OK;

        return ValidatedEntity.newBuilder()
                       .setRequest(requestedEntity)
                       .setValidationResult(result)
                       .build();
    }
}
