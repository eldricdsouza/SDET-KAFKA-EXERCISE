package com.example.orderservice.config;

import com.example.orderservice.kafka.PaymentCompletedEvent;
import com.example.orderservice.kafka.PaymentFailedEvent;
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
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent>
    paymentCompletedKafkaListenerContainerFactory() {
        return listenerContainerFactory(PaymentCompletedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentFailedEvent>
    paymentFailedKafkaListenerContainerFactory() {
        return listenerContainerFactory(PaymentFailedEvent.class);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerContainerFactory(Class<T> eventType) {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties(sslBundles);
        ConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                new JsonDeserializer<>(eventType, false)
        );

        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
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
