package com.example.orderservice.kafka;

public record PaymentFailedEvent(
        String eventId,
        String eventType,
        String orderId,
        String reason
) {
}
