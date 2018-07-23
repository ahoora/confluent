package akfak;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ComplexProducerConfig {

//    @Value("${spring.kafka.bootstrap-servers}")
//    private String bootstrapServers;

    @Autowired
    private KafkaProperties properties;

    private Map<String, Object> common() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers().get(0));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    private Map<String, Object> commonConsumer() {
        Map<String, Object> props = common();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        return props;
    }

    @Bean
    public ProducerFactory<String, Integer> producerFactoryForIntegers() {
        Map<String, Object> props = common();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ProducerFactory<String, String> producerFactoryForStrings() {
        Map<String, Object> props = common();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Integer> kafkaTemplateForIntegers() {
        return new KafkaTemplate<>(producerFactoryForIntegers());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateForStrings() {
        return new KafkaTemplate<>(producerFactoryForStrings());
    }

    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(commonConsumer(), new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConsumerFactory<String, Integer> recordConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(commonConsumer(), new StringDeserializer(), new IntegerDeserializer());
    }

    @Bean
    public KafkaListenerContainerFactory recordListenerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(recordConsumerFactory());
        return factory;
    }

    @Bean
    public KafkaListenerContainerFactory kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(stringConsumerFactory());
        return factory;
    }
}
