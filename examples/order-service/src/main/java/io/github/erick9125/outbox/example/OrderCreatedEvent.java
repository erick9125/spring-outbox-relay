package io.github.erick9125.outbox.example;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderCreatedEvent(UUID orderId, String customerId, BigDecimal total) {

  public static OrderCreatedEvent from(Order order) {
    return new OrderCreatedEvent(order.id(), order.customerId(), order.total());
  }
}
