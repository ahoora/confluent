package prozess.collaboration.specific;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import prozess.model.RequestedEntity;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Autowired
    private KafkaProperties properties;

    private Map<String, Object> common() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers().get(0));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactoryForEntityRequests() {
        final SpecificAvroSerializer<RequestedEntity> serializer = new SpecificAvroSerializer<>();
        Map<String, Object> props = common();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getClass());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new DefaultKafkaProducerFactory<>(props);
    }
}
