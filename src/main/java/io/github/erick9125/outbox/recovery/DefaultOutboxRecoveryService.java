package io.github.erick9125.outbox.recovery;

import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultOutboxRecoveryService implements OutboxRecoveryService {

  private static final Logger log = LoggerFactory.getLogger(DefaultOutboxRecoveryService.class);

  private final OutboxRepository repository;
  private final OutboxProperties properties;
  private final OutboxMetrics metrics;
  private final Clock clock;

  public DefaultOutboxRecoveryService(
      OutboxRepository repository, OutboxProperties properties, OutboxMetrics metrics) {
    this(repository, properties, metrics, Clock.systemUTC());
  }

  public DefaultOutboxRecoveryService(
      OutboxRepository repository,
      OutboxProperties properties,
      OutboxMetrics metrics,
      Clock clock) {
    this.repository = Objects.requireNonNull(repository);
    this.properties = Objects.requireNonNull(properties);
    this.metrics = Objects.requireNonNull(metrics);
    this.clock = Objects.requireNonNull(clock);
  }

  /**
   * Drains abandoned claims in bounded batches.
   *
   * <p>Each statement is capped at {@code maintenance-batch-size} so a large backlog cannot become
   * one long-running UPDATE that holds locks across the whole set. The loop keeps going until a
   * batch comes back short, which means the backlog is drained.
   */
  @Override
  public int recoverAbandonedEvents() {
    Instant lockedBefore = clock.instant().minus(properties.lockTimeout());
    int batchSize = properties.maintenanceBatchSize();
    int total = 0;

    int recovered;
    do {
      recovered = repository.recoverAbandoned(lockedBefore, batchSize);
      total += recovered;
    } while (recovered == batchSize);

    if (total > 0) {
      metrics.incrementRecovered(total);
      log.info("Recovered {} abandoned outbox events locked before {}", total, lockedBefore);
    }
    return total;
  }
}
