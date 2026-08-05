package io.github.erick9125.outbox.observability;

import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

public final class OutboxMetrics {

  private final MeterRegistry meterRegistry;
  private final OutboxRepository repository;
  private final Clock clock;

  public OutboxMetrics(MeterRegistry meterRegistry, OutboxRepository repository) {
    this(meterRegistry, repository, Clock.systemUTC());
  }

  public OutboxMetrics(MeterRegistry meterRegistry, OutboxRepository repository, Clock clock) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry);
    this.repository = Objects.requireNonNull(repository);
    this.clock = Objects.requireNonNull(clock);
    meterRegistry.gauge("outbox.pending.count", this, metrics -> metrics.repository.countPending());
    meterRegistry.gauge(
        "outbox.oldest.pending.age",
        this,
        metrics ->
            metrics
                .repository
                .findOldestPendingAvailableAt()
                .map(
                    instant ->
                        Math.max(0L, Duration.between(instant, metrics.clock.instant()).toMillis()))
                .orElse(0L));
  }

  public void incrementCreated(String destination, String eventType) {
    counter("outbox.events.created", destination, eventType, null).increment();
  }

  public void incrementClaimed(int count) {
    Counter.builder("outbox.events.claimed").register(meterRegistry).increment(count);
  }

  public void incrementPublished(String destination, String eventType) {
    counter("outbox.events.published", destination, eventType, "published").increment();
  }

  public void incrementRescheduled(String destination, String eventType) {
    counter("outbox.events.rescheduled", destination, eventType, "rescheduled").increment();
  }

  public void incrementFailed(String destination, String eventType) {
    counter("outbox.events.failed", destination, eventType, "failed").increment();
  }

  /**
   * Events whose claim expired mid-flight and was taken over by another instance. A sustained
   * non-zero rate means batches are not completing within {@code lock-timeout}.
   */
  public void incrementLockLost(String destination, String eventType) {
    counter("outbox.events.lock.lost", destination, eventType, "lock-lost").increment();
  }

  public void incrementRecovered(int count) {
    Counter.builder("outbox.events.recovered").register(meterRegistry).increment(count);
  }

  public PublicationResult recordPublication(
      String destination, String eventType, Supplier<PublicationResult> publisher) {
    Timer.Sample sample = Timer.start(meterRegistry);
    try {
      return publisher.get();
    } finally {
      sample.stop(
          Timer.builder("outbox.publication.duration")
              .tag("destination", safe(destination))
              .tag("event_type", safe(eventType))
              .register(meterRegistry));
    }
  }

  private Counter counter(String name, String destination, String eventType, String result) {
    Counter.Builder builder =
        Counter.builder(name)
            .tag("destination", safe(destination))
            .tag("event_type", safe(eventType));
    if (result != null) {
      builder.tag("result", result);
    }
    return builder.register(meterRegistry);
  }

  private static String safe(String value) {
    return value == null || value.isBlank() ? "unknown" : value;
  }
}
