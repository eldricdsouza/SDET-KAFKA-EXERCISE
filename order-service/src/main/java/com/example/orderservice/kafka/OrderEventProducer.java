package com.example.orderservice.kafka;

import com.example.orderservice.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(Order order) {
        var event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                "OrderCreated",
                order.getOrderId(),
                order.getCustomerId(),
                order.getAmount(),
                order.getCurrency()
        );

        Message<OrderCreatedEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.KEY, order.getOrderId())
                .setHeader(KafkaHeaders.TOPIC, "orders.created")
                .build();

        kafkaTemplate.send(message);
        log.info("Published OrderCreated event for orderId={}, customerId={}, amount={}, currency={}",
                order.getOrderId(), order.getCustomerId(), order.getAmount(), order.getCurrency());
    }
}
