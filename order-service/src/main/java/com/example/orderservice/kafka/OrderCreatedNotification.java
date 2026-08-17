package com.example.orderservice.kafka;

import java.math.BigDecimal;

/**
 * Application event raised while an order is being created. It is consumed only
 * after the surrounding database transaction has committed successfully.
 */
public record OrderCreatedNotification(
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency
) {
}
