package io.github.erick9125.outbox.example;

import java.math.BigDecimal;

public record CreateOrderRequest(String customerId, BigDecimal total) {}
