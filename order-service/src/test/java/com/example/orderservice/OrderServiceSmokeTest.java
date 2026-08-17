package com.example.orderservice;

import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class OrderServiceSmokeTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void contextLoads() {
        assertThat(orderService).isNotNull();
    }

    @Test
    void createOrderPersistsAndReturnsPendingStatus() {
        var request = new OrderRequest("CUST-001", new BigDecimal("100.50"), "EUR");

        var response = orderService.createOrder(request);

        assertThat(response.orderId()).isNotBlank();
        assertThat(response.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(orderRepository.existsById(response.orderId())).isTrue();
    }
}
