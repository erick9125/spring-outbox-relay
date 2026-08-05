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

  @Override
  public int recoverAbandonedEvents() {
    Instant lockedBefore = clock.instant().minus(properties.lockTimeout());
    int recovered = repository.recoverAbandoned(lockedBefore);
    if (recovered > 0) {
      metrics.incrementRecovered(recovered);
      log.info("Recovered {} abandoned outbox events locked before {}", recovered, lockedBefore);
    }
    return recovered;
  }
}
