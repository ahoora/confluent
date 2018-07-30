package prozess.serde;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import prozess.model.ValidatedEntity;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SerdeConfiguration {

    @Value("${spring.cloud.stream.kafka.streams.binder.configuration.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public GenericAvroSerde avroSerde() {
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        GenericAvroSerde avroSerde = new GenericAvroSerde();
        avroSerde.configure(serdeConfig, false);
        return avroSerde;
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SpecificAvroSerde<ValidatedEntity> validatedEntitySerde() {
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        SpecificAvroSerde<ValidatedEntity> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, false);
        return serde;
    }
}
