package com.example.paymentservice.kafka;

public record PaymentFailedEvent(
        String eventId,
        String eventType,
        String orderId,
        String reason
) {
}
