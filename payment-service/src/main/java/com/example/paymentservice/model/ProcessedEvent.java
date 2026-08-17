package com.example.paymentservice.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String orderId) {
        this.eventId = eventId;
        this.orderId = orderId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getOrderId() {
        return orderId;
    }
}
