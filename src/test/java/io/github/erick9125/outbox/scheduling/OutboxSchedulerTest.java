package io.github.erick9125.outbox.scheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.domain.RelayResult;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OutboxSchedulerTest {

  @Test
  void runsEveryJobOnItsOwnInterval() throws Exception {
    AtomicInteger relayRuns = new AtomicInteger();
    AtomicInteger recoveryRuns = new AtomicInteger();
    AtomicInteger cleanupRuns = new AtomicInteger();
    CountingRepository repository = new CountingRepository();
    OutboxMetrics metrics = new OutboxMetrics(new SimpleMeterRegistry(), repository);

    OutboxScheduler scheduler =
        new OutboxScheduler(
            () -> {
              relayRuns.incrementAndGet();
              return new RelayResult(0, 0, 0, 0, 0);
            },
            recoveryRuns::incrementAndGet,
            cleanupRuns::incrementAndGet,
            metrics,
            properties(Duration.ofMillis(20)));

    scheduler.start();
    try {
      assertThat(scheduler.isRunning()).isTrue();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                assertThat(relayRuns.get()).isPositive();
                assertThat(recoveryRuns.get()).isPositive();
                assertThat(cleanupRuns.get()).isPositive();
                // The backlog job is what refreshes the gauges now, instead of the scrape.
                assertThat(repository.countPendingCalls.get()).isPositive();
              });
    } finally {
      scheduler.stop();
    }

    assertThat(scheduler.isRunning()).isFalse();
    int relayRunsAtStop = relayRuns.get();
    Thread.sleep(150);
    assertThat(relayRuns.get()).isEqualTo(relayRunsAtStop);
  }

  @Test
  void keepsSchedulingAfterAJobThrows() {
    // A raw ScheduledExecutorService drops a repeating task the first time it throws. A relay that
    // stops polling because the database blipped once would be a silent outage.
    AtomicInteger attempts = new AtomicInteger();
    CountingRepository repository = new CountingRepository();

    OutboxScheduler scheduler =
        new OutboxScheduler(
            () -> {
              attempts.incrementAndGet();
              throw new IllegalStateException("database is down");
            },
            () -> 0,
            () -> 0,
            new OutboxMetrics(new SimpleMeterRegistry(), repository),
            properties(Duration.ofMillis(20)));

    scheduler.start();
    try {
      await().atMost(Duration.ofSeconds(5)).until(() -> attempts.get() >= 3);
    } finally {
      scheduler.stop();
    }
  }

  @Test
  void letsTheRunInProgressFinishOnStop() throws Exception {
    // Killing the relay mid-publication leaves its claims in PROCESSING until lock-timeout elapses,
    // which on every deploy means minutes of delay for those events.
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);

    OutboxScheduler scheduler =
        new OutboxScheduler(
            () -> {
              started.countDown();
              try {
                Thread.sleep(300);
              } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("relay was interrupted on shutdown", exception);
              }
              finished.countDown();
              return new RelayResult(0, 0, 0, 0, 0);
            },
            () -> 0,
            () -> 0,
            new OutboxMetrics(new SimpleMeterRegistry(), new CountingRepository()),
            properties(Duration.ofMillis(20)));

    scheduler.start();
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    scheduler.stop();

    assertThat(finished.getCount()).isZero();
  }

  @Test
  void startAndStopAreIdempotent() {
    OutboxScheduler scheduler =
        new OutboxScheduler(
            () -> new RelayResult(0, 0, 0, 0, 0),
            () -> 0,
            () -> 0,
            new OutboxMetrics(new SimpleMeterRegistry(), new CountingRepository()),
            properties(Duration.ofSeconds(1)));

    scheduler.stop();
    scheduler.start();
    scheduler.start();
    assertThat(scheduler.isRunning()).isTrue();
    scheduler.stop();
    scheduler.stop();
    assertThat(scheduler.isRunning()).isFalse();
  }

  private static OutboxProperties properties(Duration interval) {
    return new OutboxProperties(
        true,
        100,
        interval,
        Duration.ofMinutes(5),
        5,
        "scheduler-test",
        interval,
        500,
        Duration.ofSeconds(30),
        interval,
        Duration.ofSeconds(5),
        OutboxProperties.Retry.defaults(),
        new OutboxProperties.Cleanup(true, Duration.ofDays(7), interval));
  }

  /** Records how often the backlog gauges are refreshed, without touching a database. */
  private static final class CountingRepository implements OutboxRepository {

    private final AtomicInteger countPendingCalls = new AtomicInteger();

    @Override
    public UUID insert(
        io.github.erick9125.outbox.api.OutboxMessage message,
        String payloadJson,
        String headersJson,
        Instant occurredAt) {
      return UUID.randomUUID();
    }

    @Override
    public List<io.github.erick9125.outbox.domain.OutboxEvent> claimBatch(
        int batchSize, String lockedBy) {
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
    public int recoverAbandoned(Instant lockedBefore, int limit) {
      return 0;
    }

    @Override
    public int deletePublishedBefore(Instant publishedBefore, int limit) {
      return 0;
    }

    @Override
    public long countPending() {
      countPendingCalls.incrementAndGet();
      return 7L;
    }

    @Override
    public Optional<Instant> findOldestPendingAvailableAt() {
      return Optional.empty();
    }

    @Override
    public Optional<io.github.erick9125.outbox.domain.OutboxEvent> findById(UUID id) {
      return Optional.empty();
    }
  }
}
