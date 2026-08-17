package com.example.paymentservice.kafka;

import com.example.paymentservice.model.ProcessedEvent;
import com.example.paymentservice.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
public class OrderCreatedConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedConsumer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProcessedEventRepository processedEventRepository;

    public OrderCreatedConsumer(KafkaTemplate<String, Object> kafkaTemplate, ProcessedEventRepository processedEventRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(
            topics = "orders.created",
            groupId = "payment-service-group",
            containerFactory = "orderCreatedKafkaListenerContainerFactory"
    )
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreated event: eventId={}, orderId={}, customerId={}, amount={}, currency={}",
                event.eventId(), event.orderId(), event.customerId(), event.amount(), event.currency());

        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Duplicate OrderCreated event detected. Ignoring eventId={}", event.eventId());
            return;
        }

        processedEventRepository.save(new ProcessedEvent(event.eventId(), event.orderId()));

        if ("FAIL-PAYMENT".equalsIgnoreCase(event.customerId())) {
            var failedEvent = new PaymentFailedEvent(
                    UUID.randomUUID().toString(),
                    "PaymentFailed",
                    event.orderId(),
                    "Payment declined"
            );
            log.warn("Payment failed for orderId={} because customer={} was flagged for payment failure.", event.orderId(), event.customerId());
            publish("payments.failed", event.orderId(), failedEvent);
            return;
        }

        var completedEvent = new PaymentCompletedEvent(
                UUID.randomUUID().toString(),
                "PaymentCompleted",
                event.orderId(),
                event.amount(),
                event.currency()
        );
        log.info("Payment succeeded for orderId={}, amount={}, currency={}", event.orderId(), event.amount(), event.currency());
        publish("payments.completed", event.orderId(), completedEvent);
    }

    private void publish(String topic, String orderId, Object event) {
        Message<Object> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.KEY, orderId)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();
        kafkaTemplate.send(message);
        log.info("Published {} event for orderId={} with key={}", topic, orderId, orderId);
    }
}
