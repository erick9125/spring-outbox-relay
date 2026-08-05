package io.github.erick9125.outbox.cleanup;

import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultOutboxCleanupService implements OutboxCleanupService {

  private static final Logger log = LoggerFactory.getLogger(DefaultOutboxCleanupService.class);

  private final OutboxRepository repository;
  private final OutboxProperties properties;
  private final Clock clock;

  public DefaultOutboxCleanupService(OutboxRepository repository, OutboxProperties properties) {
    this(repository, properties, Clock.systemUTC());
  }

  public DefaultOutboxCleanupService(
      OutboxRepository repository, OutboxProperties properties, Clock clock) {
    this.repository = Objects.requireNonNull(repository);
    this.properties = Objects.requireNonNull(properties);
    this.clock = Objects.requireNonNull(clock);
  }

  /**
   * Deletes expired published rows in bounded batches.
   *
   * <p>An unbounded DELETE over months of retained events is one enormous transaction: long locks,
   * table bloat, and contention with the relay on the same table. Batching keeps each statement
   * short and lets the relay make progress in between.
   */
  @Override
  public int cleanupPublishedEvents() {
    if (!properties.cleanup().enabled()) {
      return 0;
    }
    Instant publishedBefore = clock.instant().minus(properties.cleanup().retention());
    int batchSize = properties.maintenanceBatchSize();
    int total = 0;

    int deleted;
    do {
      deleted = repository.deletePublishedBefore(publishedBefore, batchSize);
      total += deleted;
    } while (deleted == batchSize);

    if (total > 0) {
      log.info("Deleted {} published outbox events older than {}", total, publishedBefore);
    }
    return total;
  }
}
