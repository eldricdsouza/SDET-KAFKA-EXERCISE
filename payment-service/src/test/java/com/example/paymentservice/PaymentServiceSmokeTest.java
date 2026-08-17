package com.example.paymentservice;

import com.example.paymentservice.model.ProcessedEvent;
import com.example.paymentservice.repository.ProcessedEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class PaymentServiceSmokeTest {

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void contextLoads() {
        assertThat(processedEventRepository).isNotNull();
    }

    @Test
    void processedEventRepositoryCanPersist() {
        var event = new ProcessedEvent("evt-123", "order-123");

        processedEventRepository.save(event);

        assertThat(processedEventRepository.existsById("evt-123")).isTrue();
    }
}
