package com.example.paymentservice.kafka;

import java.math.BigDecimal;

public record PaymentCompletedEvent(
        String eventId,
        String eventType,
        String orderId,
        BigDecimal amount,
        String currency
) {
}
