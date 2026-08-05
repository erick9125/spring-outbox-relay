package io.github.erick9125.outbox.example;

import java.math.BigDecimal;
import java.util.UUID;

public record Order(UUID id, String customerId, BigDecimal total) {

  public static Order create(String customerId, BigDecimal total) {
    return new Order(UUID.randomUUID(), customerId, total);
  }
}
