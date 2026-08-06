package io.github.erick9125.outbox.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OutboxMetricsTest {

  private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");

  private final StubRepository repository = new StubRepository();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final OutboxMetrics metrics =
      new OutboxMetrics(registry, repository, Clock.fixed(NOW, ZoneOffset.UTC));

  @Test
  void doesNotQueryTheDatabaseWhenGaugesAreRead() {
    // The gauges used to run COUNT(*) and an ORDER BY on every scrape, on the scrape thread and
    // with
    // a pooled connection, so a slow database could hang the metrics endpoint and each extra
    // collector multiplied the cost.
    gauge("outbox.pending.count");
    gauge("outbox.oldest.pending.age");
    gauge("outbox.pending.count");

    assertThat(repository.countPendingCalls.get()).isZero();
    assertThat(repository.oldestPendingCalls.get()).isZero();
  }

  @Test
  void publishesTheBacklogReadByTheRefreshJob() {
    repository.pending = 42L;
    repository.oldestAvailableAt = NOW.minus(Duration.ofMinutes(3));

    metrics.refreshBacklog();

    assertThat(gauge("outbox.pending.count")).isEqualTo(42.0);
    assertThat(gauge("outbox.oldest.pending.age")).isEqualTo(180_000.0);
    assertThat(repository.countPendingCalls.get()).isEqualTo(1);
  }

  @Test
  void reportsZeroAgeWhenThereIsNoBacklog() {
    repository.pending = 0L;
    repository.oldestAvailableAt = null;

    metrics.refreshBacklog();

    assertThat(gauge("outbox.pending.count")).isZero();
    assertThat(gauge("outbox.oldest.pending.age")).isZero();
  }

  @Test
  void clampsTheAgeOfAnEventScheduledForTheFuture() {
    // A rescheduled event has available_at in the future, which would otherwise read as a negative
    // backlog age.
    repository.oldestAvailableAt = NOW.plus(Duration.ofMinutes(5));

    metrics.refreshBacklog();

    assertThat(gauge("outbox.oldest.pending.age")).isZero();
  }

  @Test
  void keepsTheLastValueWhenTheDatabaseIsUnreachable() {
    repository.pending = 12L;
    metrics.refreshBacklog();
    repository.failing = true;

    metrics.refreshBacklog();

    // Stale beats broken: a database hiccup must not take down the job that reports the backlog.
    assertThat(gauge("outbox.pending.count")).isEqualTo(12.0);
  }

  private double gauge(String name) {
    return registry.get(name).gauge().value();
  }

  private static final class StubRepository implements OutboxRepository {

    private final AtomicInteger countPendingCalls = new AtomicInteger();
    private final AtomicInteger oldestPendingCalls = new AtomicInteger();
    private long pending;
    private Instant oldestAvailableAt;
    private boolean failing;

    @Override
    public long countPending() {
      countPendingCalls.incrementAndGet();
      if (failing) {
        throw new IllegalStateException("database is unreachable");
      }
      return pending;
    }

    @Override
    public Optional<Instant> findOldestPendingAvailableAt() {
      oldestPendingCalls.incrementAndGet();
      if (failing) {
        throw new IllegalStateException("database is unreachable");
      }
      return Optional.ofNullable(oldestAvailableAt);
    }

    @Override
    public UUID insert(
        OutboxMessage message, String payloadJson, String headersJson, Instant occurredAt) {
      return UUID.randomUUID();
    }

    @Override
    public List<OutboxEvent> claimBatch(int batchSize, String lockedBy) {
      return List.of();
    }

    @Override
    public boolean markPublished(
        UUID id, String lockedBy, Instant publishedAt, String brokerMessageId) {
      return true;
    }

    @Override
    public boolean reschedule(
        UUID id, String lockedBy, int attempts, Instant availableAt, String lastError) {
      return true;
    }

    @Override
    public boolean markFailed(UUID id, String lockedBy, int attempts, String lastError) {
      return true;
    }

    @Override
    public int recoverAbandoned(Instant lockedBefore, int maxRecoveries, int limit) {
      return 0;
    }

    @Override
    public int deletePublishedBefore(Instant publishedBefore, int limit) {
      return 0;
    }

    @Override
    public int failExhaustedRecoveries(Instant lockedBefore, int maxRecoveries, int limit) {
      return 0;
    }

    @Override
    public Optional<OutboxEvent> findById(UUID id) {
      return Optional.empty();
    }
  }
}
