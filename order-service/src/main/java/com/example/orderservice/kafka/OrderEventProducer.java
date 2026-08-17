package com.example.orderservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publishOrderCreated(OrderCreatedNotification notification) {
        var event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                "OrderCreated",
                notification.orderId(),
                notification.customerId(),
                notification.amount(),
                notification.currency()
        );

        Message<OrderCreatedEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.KEY, notification.orderId())
                .setHeader(KafkaHeaders.TOPIC, "orders.created")
                .build();

        kafkaTemplate.send(message);
        log.info("Published OrderCreated event for orderId={}, customerId={}, amount={}, currency={}",
                notification.orderId(), notification.customerId(), notification.amount(), notification.currency());
    }
}
