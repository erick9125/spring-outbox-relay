package io.github.erick9125.outbox.observability;

import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OutboxMetrics {

  private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

  private final MeterRegistry meterRegistry;
  private final OutboxRepository repository;
  private final Clock clock;

  private final AtomicLong pendingCount = new AtomicLong();
  private final AtomicLong oldestPendingAgeMillis = new AtomicLong();

  public OutboxMetrics(MeterRegistry meterRegistry, OutboxRepository repository) {
    this(meterRegistry, repository, Clock.systemUTC());
  }

  public OutboxMetrics(MeterRegistry meterRegistry, OutboxRepository repository, Clock clock) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry);
    this.repository = Objects.requireNonNull(repository);
    this.clock = Objects.requireNonNull(clock);

    // The gauges read cached values. They used to query the database on every scrape, which put a
    // COUNT(*) over the outbox table — and a connection from the pool — on the scrape thread, so a
    // slow database could hang /actuator/prometheus, and every extra Prometheus server multiplied
    // the load. A Supplier-based gauge also keeps a strong reference, where the object-and-function
    // form holds the state weakly and silently reports NaN once it is collected.
    Gauge.builder("outbox.pending.count", () -> (double) pendingCount.get())
        .description("Events waiting to be relayed")
        .register(meterRegistry);
    Gauge.builder("outbox.oldest.pending.age", () -> (double) oldestPendingAgeMillis.get())
        .description("Age of the oldest pending event")
        .baseUnit("milliseconds")
        .register(meterRegistry);
  }

  /**
   * Re-reads the backlog into the cached gauges.
   *
   * <p>Driven by {@code backlog-metrics-interval} rather than by scrapes, so the cost is fixed no
   * matter how many collectors are pointed at the application. Failures are logged and swallowed: a
   * database hiccup should leave the gauges stale, not break the job that reports them.
   */
  public void refreshBacklog() {
    try {
      pendingCount.set(repository.countPending());
      oldestPendingAgeMillis.set(
          repository
              .findOldestPendingAvailableAt()
              .map(instant -> Math.max(0L, Duration.between(instant, clock.instant()).toMillis()))
              .orElse(0L));
    } catch (RuntimeException exception) {
      log.warn("Could not refresh outbox backlog metrics; gauges keep their last value", exception);
    }
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

  /**
   * Events retired because their claim was abandoned more times than {@code max-recoveries} allows.
   * Any non-zero value is worth an alert: something about those events keeps outliving the lock
   * timeout, and they are no longer being delivered.
   */
  public void incrementRecoveryExhausted(int count) {
    Counter.builder("outbox.events.recovery.exhausted").register(meterRegistry).increment(count);
  }

  /**
   * Records how long the broker took to acknowledge one event.
   *
   * <p>Tagged with the outcome so that failures — a 30 second timeout, most of all — do not sit in
   * the same distribution as successful publications and drag the success percentiles with them.
   */
  public void recordPublicationDuration(
      String destination, String eventType, boolean success, Duration duration) {
    Timer.builder("outbox.publication.duration")
        .tag("destination", safe(destination))
        .tag("event_type", safe(eventType))
        .tag("result", success ? "success" : "failure")
        .register(meterRegistry)
        .record(duration);
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
