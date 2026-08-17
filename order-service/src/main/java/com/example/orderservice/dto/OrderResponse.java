package com.example.orderservice.dto;

import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;

import java.math.BigDecimal;

public record OrderResponse(
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency,
        OrderStatus status
) {
    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getOrderId(),
                order.getCustomerId(),
                order.getAmount(),
                order.getCurrency(),
                order.getStatus()
        );
    }
}
