package com.example.paymentservice.config;

import com.example.paymentservice.kafka.OrderCreatedEvent;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final SslBundles sslBundles;
    private final ConcurrentKafkaListenerContainerFactoryConfigurer factoryConfigurer;

    public KafkaConsumerConfig(
            KafkaProperties kafkaProperties,
            SslBundles sslBundles,
            ConcurrentKafkaListenerContainerFactoryConfigurer factoryConfigurer
    ) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
        this.factoryConfigurer = factoryConfigurer;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>
    orderCreatedKafkaListenerContainerFactory() {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties(sslBundles);
        ConsumerFactory<String, OrderCreatedEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                new JsonDeserializer<>(OrderCreatedEvent.class, false)
        );

        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        configure(factory, consumerFactory);
        return factory;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void configure(
            ConcurrentKafkaListenerContainerFactory<String, ?> factory,
            ConsumerFactory<String, ?> consumerFactory
    ) {
        factoryConfigurer.configure((ConcurrentKafkaListenerContainerFactory) factory, (ConsumerFactory) consumerFactory);
    }
}
