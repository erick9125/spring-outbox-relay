package io.github.erick9125.outbox.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.support.AbstractPostgresIntegrationTest;
import io.github.erick9125.outbox.support.OutboxTestSupport;
import io.github.erick9125.outbox.support.OutboxTestSupport.Fixture;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcOutboxRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

  private Fixture fixture;

  @BeforeEach
  void setUp() {
    fixture = OutboxTestSupport.fixture(POSTGRES);
    fixture.jdbcTemplate().update("DELETE FROM outbox_event");
  }

  @Test
  void persistsEventWithPayloadAndHeaders() {
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("order-1")
                .eventType("order.created")
                .eventVersion(1)
                .destination("orders.events")
                .partitionKey("order-1")
                .payload(Map.of("total", 42))
                .header("correlation-id", "abc")
                .build());

    OutboxEvent event = fixture.repository().findById(id).orElseThrow();
    assertThat(event.status()).isEqualTo(OutboxStatus.PENDING);
    assertThat(event.payload()).contains("42");
    assertThat(event.headers()).contains("correlation-id");
    assertThat(event.destination()).isEqualTo("orders.events");
  }

  @Test
  void marksPublishedWithoutABrokerMessageIdAndKeepsExistingHeaders() {
    // Not every broker returns a message id. jsonb_set is strict, so a null id used to collapse the
    // whole headers column to NULL, violate its NOT NULL constraint, and make the relay treat an
    // already-published event as a retryable failure — republishing it on every attempt.
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("order-4")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("total", 7))
                .header("correlation-id", "keep-me")
                .build());

    fixture.repository().claimBatch(1, "worker-a");

    assertThat(fixture.repository().markPublished(id, "worker-a", Instant.now(), null)).isTrue();

    OutboxEvent event = fixture.repository().findById(id).orElseThrow();
    assertThat(event.status()).isEqualTo(OutboxStatus.PUBLISHED);
    assertThat(event.headers()).isNotNull();
    assertThat(event.headers()).contains("correlation-id").contains("keep-me");
    assertThat(event.publishedAt()).isNotNull();
  }

  @Test
  void refusesToPublishWithoutAnActiveTransaction() {
    // Outside a transaction the insert would commit on its own and the atomicity guarantee the
    // whole pattern exists for would be silently gone. Note this calls the publisher directly:
    // Fixture.publish() opens a transaction on purpose.
    assertThatThrownBy(
            () ->
                fixture
                    .publisher()
                    .publish(
                        OutboxMessage.builder()
                            .aggregateType("ORDER")
                            .aggregateId("order-5")
                            .eventType("order.created")
                            .destination("orders.events")
                            .payload(Map.of("ok", true))
                            .build()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("requires an active transaction")
        .hasMessageContaining("order.created")
        .hasMessageContaining("ORDER/order-5");

    assertThat(fixture.repository().countPending()).isZero();
  }

  @Test
  void rollsBackOutboxWithBusinessTransaction() {
    AtomicReference<UUID> id = new AtomicReference<>();

    try {
      fixture
          .transactionTemplate()
          .executeWithoutResult(
              status -> {
                id.set(
                    fixture.publish(
                        OutboxMessage.builder()
                            .aggregateType("ORDER")
                            .aggregateId("order-2")
                            .eventType("order.created")
                            .destination("orders.events")
                            .payload(Map.of("ok", true))
                            .build()));
                throw new IllegalStateException("business failure");
              });
    } catch (IllegalStateException ignored) {
      // expected
    }

    assertThat(id.get()).isNotNull();
    assertThat(fixture.repository().findById(id.get())).isEmpty();
  }

  @Test
  void commitsOutboxWithBusinessTransaction() {
    UUID id =
        fixture
            .transactionTemplate()
            .execute(
                status ->
                    fixture.publish(
                        OutboxMessage.builder()
                            .aggregateType("ORDER")
                            .aggregateId("order-3")
                            .eventType("order.created")
                            .destination("orders.events")
                            .payload(Map.of("ok", true))
                            .build()));

    assertThat(fixture.repository().findById(id)).isPresent();
  }
}
