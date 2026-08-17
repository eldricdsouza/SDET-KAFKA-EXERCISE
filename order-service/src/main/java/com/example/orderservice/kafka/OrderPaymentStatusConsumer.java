package com.example.orderservice.kafka;

import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OrderPaymentStatusConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderPaymentStatusConsumer.class);
    private final OrderService orderService;

    public OrderPaymentStatusConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "payments.completed",
            groupId = "order-service-group",
            containerFactory = "paymentCompletedKafkaListenerContainerFactory"
    )
    public void consumePaymentCompleted(@Payload PaymentCompletedEvent event) {
        log.info("Received payments.completed event: eventId={}, orderId={}", event.eventId(), event.orderId());
        try {
            orderService.updateOrderStatus(event.orderId(), OrderStatus.PAID);
            log.info("Order {} marked as PAID", event.orderId());
        } catch (OrderNotFoundException ex) {
            log.warn("Ignoring payment completion for unknown order {}: {}", event.orderId(), ex.getMessage());
        }
    }

    @KafkaListener(
            topics = "payments.failed",
            groupId = "order-service-group",
            containerFactory = "paymentFailedKafkaListenerContainerFactory"
    )
    public void consumePaymentFailed(@Payload PaymentFailedEvent event) {
        log.info("Received payments.failed event: eventId={}, orderId={}, reason={}", event.eventId(), event.orderId(), event.reason());
        try {
            orderService.updateOrderStatus(event.orderId(), OrderStatus.PAYMENT_FAILED);
            log.info("Order {} marked as PAYMENT_FAILED", event.orderId());
        } catch (OrderNotFoundException ex) {
            log.warn("Ignoring payment failure for unknown order {}: {}", event.orderId(), ex.getMessage());
        }
    }
}
