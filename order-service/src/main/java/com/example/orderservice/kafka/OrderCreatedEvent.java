package com.example.orderservice.kafka;

import java.math.BigDecimal;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency
) {
}
