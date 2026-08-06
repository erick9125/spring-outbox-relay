package io.github.erick9125.outbox.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxMessageTest {

  @Test
  void rejectsValuesTooLongForTheirColumn() {
    // Left to the insert, an over-long value came back as a raw SQL error that took the caller's
    // business transaction down with it, naming a column rather than the builder field.
    assertThatThrownBy(() -> valid().aggregateType("A".repeat(101)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("aggregateType")
        .hasMessageContaining("at most 100")
        .hasMessageContaining("was 101");

    assertThatThrownBy(() -> valid().aggregateId("a".repeat(151)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("aggregateId");

    assertThatThrownBy(() -> valid().eventType("e".repeat(151)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("eventType");

    assertThatThrownBy(() -> valid().destination("d".repeat(201)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("destination");

    assertThatThrownBy(() -> valid().partitionKey("p".repeat(201)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partitionKey");
  }

  @Test
  void acceptsValuesExactlyAtTheLimit() {
    OutboxMessage message =
        valid()
            .aggregateType("A".repeat(100))
            .aggregateId("a".repeat(150))
            .eventType("e".repeat(150))
            .destination("d".repeat(200))
            .partitionKey("p".repeat(200))
            .build();

    assertThat(message.aggregateType()).hasSize(100);
    assertThat(message.destination()).hasSize(200);
  }

  @Test
  void allowsAnAbsentPartitionKey() {
    assertThat(valid().build().partitionKey()).isNull();
  }

  @Test
  void copiesAndFreezesTheHeaders() {
    Map<String, String> mutable = new LinkedHashMap<>();
    mutable.put("correlation-id", "abc");

    OutboxMessage message = valid().headers(mutable).build();
    mutable.put("added-later", "nope");

    assertThat(message.headers()).containsExactly(Map.entry("correlation-id", "abc"));
    assertThatThrownBy(() -> message.headers().put("k", "v"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static OutboxMessage.Builder valid() {
    return OutboxMessage.builder()
        .aggregateType("ORDER")
        .aggregateId("order-1")
        .eventType("order.created")
        .destination("orders.events")
        .payload(Map.of("ok", true));
  }
}
