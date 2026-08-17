package com.example.orderservice.service;

import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.kafka.OrderEventProducer;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        log.info("Creating order for customerId={}, amount={}, currency={}", request.getCustomerId(), request.getAmount(), request.getCurrency());

        var orderId = UUID.randomUUID().toString();
        var order = new Order(orderId, request.getCustomerId(), request.getAmount(), request.getCurrency().toUpperCase(), OrderStatus.PENDING);
        var saved = orderRepository.save(order);

        log.info("Order persisted: orderId={}, status={}", saved.getOrderId(), saved.getStatus());
        orderEventProducer.publishOrderCreated(saved);
        return OrderResponse.from(saved);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(String orderId) {
        var order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return OrderResponse.from(order);
    }

    @Transactional
    public void updateOrderStatus(String orderId, OrderStatus newStatus) {
        var order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        if (order.getStatus() != OrderStatus.PENDING) {
            log.info("Ignoring status update for orderId={}: currentStatus={}, requestedStatus={}",
                    orderId, order.getStatus(), newStatus);
            return;
        }

        OrderStatus previousStatus = order.getStatus();
        order.setStatus(newStatus);
        orderRepository.save(order);
        log.info("Order status changed: orderId={}, {} -> {}", orderId, previousStatus, newStatus);
    }

    @Transactional(readOnly = true)
    public boolean exists(String orderId) {
        return orderRepository.existsById(orderId);
    }
}
