package com.example.orderservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public class OrderRequest {

    @NotBlank(message = "customerId is mandatory")
    private String customerId;

    @NotNull(message = "amount is mandatory")
    @Positive(message = "amount must be greater than zero")
    private BigDecimal amount;

    @NotBlank(message = "currency is mandatory")
    @Pattern(regexp = "^[A-Za-z]{3}$", message = "currency must be a 3-character ISO-style code")
    private String currency;

    public OrderRequest() {
    }

    public OrderRequest(String customerId, BigDecimal amount, String currency) {
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
}
