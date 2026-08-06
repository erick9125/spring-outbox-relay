package io.github.erick9125.outbox.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.support.AbstractPostgresIntegrationTest;
import io.github.erick9125.outbox.support.OutboxTestSupport;
import io.github.erick9125.outbox.support.OutboxTestSupport.Fixture;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
  void appliesTheIndexesTheJobsActuallyQueryOn() {
    List<String> indexes =
        fixture
            .jdbcTemplate()
            .queryForList(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'outbox_event' ORDER BY 1",
                String.class);

    // Partial indexes covering exactly the rows each job looks at. The original single index led on
    // status, which left the polling query sorting on every run, and offered the cleanup job
    // nothing
    // to seek on at all.
    assertThat(indexes)
        .contains(
            "idx_outbox_event_pending", "idx_outbox_event_published", "idx_outbox_event_processing")
        .doesNotContain("idx_outbox_event_polling", "idx_outbox_event_recovery");
  }

  @Test
  void satisfiesThePollingOrderFromTheIndexWithoutSorting() {
    fixture.publish(message("explain-me"));

    // Sequential scans are disabled for this statement so the assertion is about the index's shape
    // rather than about what the planner picks on a table with one row. The point of leading the
    // index with available_at is that the polling query's ORDER BY comes out of the index: the old
    // index led with status, which made available_at a range predicate and forced a sort every
    // poll.
    String plan =
        fixture
            .transactionTemplate()
            .execute(
                status -> {
                  fixture.jdbcTemplate().execute("SET LOCAL enable_seqscan = off");
                  return String.join(
                      "\n",
                      fixture
                          .jdbcTemplate()
                          .queryForList(
                              """
                              EXPLAIN SELECT id FROM outbox_event
                              WHERE status = 'PENDING' AND available_at <= now()
                              ORDER BY available_at, created_at
                              """,
                              String.class));
                });

    assertThat(plan).contains("idx_outbox_event_pending");
    assertThat(plan).doesNotContain("Sort Key");
  }

  @Test
  void boundsRecoveryAndCleanupToTheRequestedLimit() {
    // The limit is what keeps a huge backlog from becoming one long-running statement holding locks
    // over the whole set. Asserting on the drain loop's total would not catch its absence, so this
    // checks a single call returns no more rows than it was asked for.
    for (int i = 0; i < 5; i++) {
      fixture.publish(message("abandoned-" + i));
    }
    fixture.repository().claimBatch(10, "dead-worker");
    fixture
        .jdbcTemplate()
        .update(
            "UPDATE outbox_event SET locked_at = ?",
            Timestamp.from(Instant.now().minus(Duration.ofMinutes(10))));

    Instant lockedBefore = Instant.now().minus(Duration.ofMinutes(1));
    assertThat(fixture.repository().recoverAbandoned(lockedBefore, 3, 2)).isEqualTo(2);
    assertThat(fixture.repository().recoverAbandoned(lockedBefore, 3, 2)).isEqualTo(2);
    assertThat(fixture.repository().recoverAbandoned(lockedBefore, 3, 2)).isEqualTo(1);
    assertThat(fixture.repository().recoverAbandoned(lockedBefore, 3, 2)).isZero();

    fixture.repository().claimBatch(10, "worker-a");
    fixture
        .jdbcTemplate()
        .update(
            "UPDATE outbox_event SET status = 'PUBLISHED', published_at = ?, locked_by = NULL",
            Timestamp.from(Instant.now().minus(Duration.ofDays(30))));

    Instant publishedBefore = Instant.now().minus(Duration.ofDays(1));
    assertThat(fixture.repository().deletePublishedBefore(publishedBefore, 3)).isEqualTo(3);
    assertThat(fixture.repository().deletePublishedBefore(publishedBefore, 3)).isEqualTo(2);
    assertThat(fixture.repository().deletePublishedBefore(publishedBefore, 3)).isZero();
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

  private static OutboxMessage message(String aggregateId) {
    return OutboxMessage.builder()
        .aggregateType("ORDER")
        .aggregateId(aggregateId)
        .eventType("order.created")
        .destination("orders.events")
        .payload(Map.of("ok", true))
        .build();
  }
}
