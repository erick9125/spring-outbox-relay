package io.github.erick9125.outbox.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.support.AbstractPostgresIntegrationTest;
import io.github.erick9125.outbox.support.OutboxTestSupport;
import io.github.erick9125.outbox.support.OutboxTestSupport.Fixture;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRecoveryServiceIntegrationTest extends AbstractPostgresIntegrationTest {

  private Fixture fixture;

  @BeforeEach
  void setUp() {
    fixture = OutboxTestSupport.fixture(POSTGRES);
    fixture.jdbcTemplate().update("DELETE FROM outbox_event");
  }

  @Test
  void recoversAbandonedLocksAndPreservesAttempts() {
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("abandoned")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("ok", true))
                .build());

    fixture.repository().claimBatch(1, "dead-worker");
    fixture
        .jdbcTemplate()
        .update(
            """
                        UPDATE outbox_event
                        SET locked_at = ?, attempts = 2
                        WHERE id = ?
                        """,
            java.sql.Timestamp.from(Instant.now().minus(Duration.ofMinutes(10))),
            id);

    OutboxProperties properties =
        new OutboxProperties(
            true,
            50,
            Duration.ofSeconds(1),
            Duration.ofMinutes(5),
            5,
            "recovery",
            Duration.ofMinutes(1),
            500,
            3,
            Duration.ofSeconds(30),
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            OutboxProperties.Retry.defaults(),
            OutboxProperties.Cleanup.defaults());
    OutboxRecoveryService recovery =
        new DefaultOutboxRecoveryService(
            fixture.repository(),
            properties,
            new OutboxMetrics(new SimpleMeterRegistry(), fixture.repository()));

    int recovered = recovery.recoverAbandonedEvents();
    assertThat(recovered).isEqualTo(1);

    OutboxEvent event = fixture.repository().findById(id).orElseThrow();
    assertThat(event.status()).isEqualTo(OutboxStatus.PENDING);
    assertThat(event.lockedBy()).isNull();
    assertThat(event.lockedAt()).isNull();
    assertThat(event.attempts()).isEqualTo(2);
  }

  @Test
  void recoversTheWholeBacklogAcrossSeveralBatches() {
    // maintenance-batch-size is 2, so 5 abandoned claims need three statements. Before batching
    // this was one unbounded UPDATE holding locks over the entire backlog.
    java.util.stream.IntStream.range(0, 5)
        .forEach(
            i ->
                fixture.publish(
                    OutboxMessage.builder()
                        .aggregateType("ORDER")
                        .aggregateId("abandoned-" + i)
                        .eventType("order.created")
                        .destination("orders.events")
                        .payload(Map.of("i", i))
                        .build()));

    fixture.repository().claimBatch(10, "dead-worker");
    fixture
        .jdbcTemplate()
        .update(
            "UPDATE outbox_event SET locked_at = ?",
            java.sql.Timestamp.from(Instant.now().minus(Duration.ofMinutes(10))));

    OutboxProperties properties =
        new OutboxProperties(
            true,
            50,
            Duration.ofSeconds(1),
            Duration.ofMinutes(5),
            5,
            "recovery",
            Duration.ofMinutes(1),
            2,
            3,
            Duration.ofSeconds(30),
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            OutboxProperties.Retry.defaults(),
            OutboxProperties.Cleanup.defaults());

    OutboxRecoveryService recovery =
        new DefaultOutboxRecoveryService(
            fixture.repository(),
            properties,
            new OutboxMetrics(new SimpleMeterRegistry(), fixture.repository()));

    assertThat(recovery.recoverAbandonedEvents()).isEqualTo(5);
    assertThat(fixture.repository().countPending()).isEqualTo(5);

    // Backlog drained: the loop must stop rather than spin.
    assertThat(recovery.recoverAbandonedEvents()).isZero();
  }

  @Test
  void retiresAnEventThatKeepsOutlivingItsLock() {
    // The poison pill: an event whose processing kills the worker used to loop forever, because
    // recovery handed it back untouched. It now spends a recovery budget and ends up FAILED, where
    // an operator can see it.
    UUID id = fixture.publish(message("poison"));

    OutboxProperties properties =
        OutboxTestSupport.props().instanceId("recovery").maxRecoveries(2).build();
    OutboxRecoveryService recovery =
        new DefaultOutboxRecoveryService(
            fixture.repository(),
            properties,
            new OutboxMetrics(new SimpleMeterRegistry(), fixture.repository()));

    // Two rounds of claim-then-abandon are within budget.
    for (int round = 1; round <= 2; round++) {
      abandonClaim();
      assertThat(recovery.recoverAbandonedEvents()).isEqualTo(1);
      OutboxEvent event = fixture.repository().findById(id).orElseThrow();
      assertThat(event.status()).isEqualTo(OutboxStatus.PENDING);
      assertThat(event.recoveries()).isEqualTo(round);
      // The publication budget is untouched: this event never failed to publish.
      assertThat(event.attempts()).isZero();
    }

    // The third abandonment is over budget, so the event is retired instead of handed back.
    abandonClaim();
    assertThat(recovery.recoverAbandonedEvents()).isZero();

    OutboxEvent retired = fixture.repository().findById(id).orElseThrow();
    assertThat(retired.status()).isEqualTo(OutboxStatus.FAILED);
    assertThat(retired.lockedBy()).isNull();
    assertThat(retired.lastError()).contains("recovery budget exhausted");
    assertThat(fixture.repository().countPending()).isZero();
  }

  @Test
  void spendsTheRecoveryBudgetInsteadOfThePublicationBudget() {
    // Deploys interrupt in-flight publications, so recoveries are normal operation. Charging them
    // to attempts would send perfectly good events to FAILED after a few releases.
    UUID id = fixture.publish(message("deployed-through"));

    OutboxRecoveryService recovery =
        new DefaultOutboxRecoveryService(
            fixture.repository(),
            OutboxTestSupport.props().instanceId("recovery").build(),
            new OutboxMetrics(new SimpleMeterRegistry(), fixture.repository()));

    abandonClaim();
    recovery.recoverAbandonedEvents();

    OutboxEvent event = fixture.repository().findById(id).orElseThrow();
    assertThat(event.attempts()).isZero();
    assertThat(event.recoveries()).isEqualTo(1);
    assertThat(event.maxAttempts()).isEqualTo(5);
  }

  /** Claims everything pending and backdates the lock past the timeout. */
  private void abandonClaim() {
    fixture.repository().claimBatch(10, "dead-worker");
    fixture
        .jdbcTemplate()
        .update(
            "UPDATE outbox_event SET locked_at = ? WHERE status = 'PROCESSING'",
            java.sql.Timestamp.from(Instant.now().minus(Duration.ofMinutes(10))));
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

  @Test
  void doesNotRecoverFreshLocks() {
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("fresh")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("ok", true))
                .build());

    fixture.repository().claimBatch(1, "alive-worker");

    OutboxRecoveryService recovery =
        new DefaultOutboxRecoveryService(
            fixture.repository(),
            fixture.properties(),
            new OutboxMetrics(new SimpleMeterRegistry(), fixture.repository()));

    assertThat(recovery.recoverAbandonedEvents()).isZero();
    assertThat(fixture.repository().findById(id).orElseThrow().status())
        .isEqualTo(OutboxStatus.PROCESSING);
  }
}
