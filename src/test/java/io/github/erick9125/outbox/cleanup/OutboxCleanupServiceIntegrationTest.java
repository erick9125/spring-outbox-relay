package io.github.erick9125.outbox.cleanup;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.support.AbstractPostgresIntegrationTest;
import io.github.erick9125.outbox.support.OutboxTestSupport;
import io.github.erick9125.outbox.support.OutboxTestSupport.Fixture;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxCleanupServiceIntegrationTest extends AbstractPostgresIntegrationTest {

  private Fixture fixture;

  @BeforeEach
  void setUp() {
    fixture = OutboxTestSupport.fixture(POSTGRES);
    fixture.jdbcTemplate().update("DELETE FROM outbox_event");
  }

  @Test
  void deletesEveryExpiredRowEvenWhenItTakesSeveralBatches() {
    // maintenance-batch-size is 2 here, so 7 expired rows need four statements. Before batching
    // this was a single unbounded DELETE over the whole retention window.
    IntStream.range(0, 7).forEach(i -> publishAndMarkPublished(Duration.ofDays(30)));
    UUID fresh = publishAndMarkPublished(Duration.ofMinutes(5));

    OutboxCleanupService cleanup =
        new DefaultOutboxCleanupService(fixture.repository(), propertiesWithBatchSize(2));

    assertThat(cleanup.cleanupPublishedEvents()).isEqualTo(7);
    assertThat(countRows()).isEqualTo(1);
    assertThat(fixture.repository().findById(fresh)).isPresent();

    // Nothing left to delete: the drain loop must stop instead of spinning.
    assertThat(cleanup.cleanupPublishedEvents()).isZero();
  }

  @Test
  void doesNothingWhenCleanupIsDisabled() {
    publishAndMarkPublished(Duration.ofDays(30));

    OutboxProperties disabled =
        new OutboxProperties(
            true,
            50,
            Duration.ofMillis(100),
            Duration.ofMinutes(5),
            5,
            "cleanup-test",
            Duration.ofSeconds(30),
            500,
            Duration.ofSeconds(30),
            OutboxProperties.Retry.defaults(),
            new OutboxProperties.Cleanup(false, Duration.ofDays(7), Duration.ofHours(1)));

    assertThat(
            new DefaultOutboxCleanupService(fixture.repository(), disabled)
                .cleanupPublishedEvents())
        .isZero();
    assertThat(countRows()).isEqualTo(1);
  }

  @Test
  void leavesPendingAndFailedRowsAlone() {
    UUID pending = fixture.publish(message("pending"));
    UUID failed = fixture.publish(message("failed"));
    fixture.repository().claimBatch(10, "worker-a");
    fixture.repository().markFailed(failed, "worker-a", 5, "exhausted");
    fixture
        .repository()
        .reschedule(pending, "worker-a", 0, Instant.now().minus(Duration.ofSeconds(1)), null);

    OutboxCleanupService cleanup =
        new DefaultOutboxCleanupService(fixture.repository(), propertiesWithBatchSize(500));

    assertThat(cleanup.cleanupPublishedEvents()).isZero();
    assertThat(countRows()).isEqualTo(2);
  }

  private OutboxProperties propertiesWithBatchSize(int maintenanceBatchSize) {
    return new OutboxProperties(
        true,
        50,
        Duration.ofMillis(100),
        Duration.ofMinutes(5),
        5,
        "cleanup-test",
        Duration.ofSeconds(30),
        maintenanceBatchSize,
        Duration.ofSeconds(30),
        OutboxProperties.Retry.defaults(),
        new OutboxProperties.Cleanup(true, Duration.ofDays(7), Duration.ofHours(1)));
  }

  private UUID publishAndMarkPublished(Duration publishedAgo) {
    UUID id = fixture.publish(message("cleanup-" + UUID.randomUUID()));
    fixture.repository().claimBatch(10, "worker-a");
    fixture.repository().markPublished(id, "worker-a", Instant.now(), "broker-1");
    fixture
        .jdbcTemplate()
        .update(
            "UPDATE outbox_event SET published_at = ? WHERE id = ?",
            Timestamp.from(Instant.now().minus(publishedAgo)),
            id);
    return id;
  }

  private OutboxMessage message(String aggregateId) {
    return OutboxMessage.builder()
        .aggregateType("ORDER")
        .aggregateId(aggregateId)
        .eventType("order.created")
        .destination("orders.events")
        .payload(Map.of("ok", true))
        .build();
  }

  private int countRows() {
    Integer count =
        fixture.jdbcTemplate().queryForObject("SELECT COUNT(*) FROM outbox_event", Integer.class);
    return count == null ? 0 : count;
  }
}
