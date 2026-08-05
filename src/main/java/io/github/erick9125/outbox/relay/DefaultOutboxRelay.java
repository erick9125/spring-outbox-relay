package io.github.erick9125.outbox.relay;

import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.domain.RelayResult;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.exception.RetryablePublicationException;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.github.erick9125.outbox.retry.RetryDecision;
import io.github.erick9125.outbox.retry.RetryPolicy;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultOutboxRelay implements OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(DefaultOutboxRelay.class);

  private final OutboxRepository repository;
  private final MessageBrokerPublisher brokerPublisher;
  private final RetryPolicy retryPolicy;
  private final OutboxProperties properties;
  private final OutboxMetrics metrics;
  private final ObservationRegistry observationRegistry;
  private final String instanceId;

  public DefaultOutboxRelay(
      OutboxRepository repository,
      MessageBrokerPublisher brokerPublisher,
      RetryPolicy retryPolicy,
      OutboxProperties properties,
      OutboxMetrics metrics,
      ObservationRegistry observationRegistry,
      String instanceId) {
    this.repository = Objects.requireNonNull(repository);
    this.brokerPublisher = Objects.requireNonNull(brokerPublisher);
    this.retryPolicy = Objects.requireNonNull(retryPolicy);
    this.properties = Objects.requireNonNull(properties);
    this.metrics = Objects.requireNonNull(metrics);
    this.observationRegistry =
        observationRegistry == null ? ObservationRegistry.NOOP : observationRegistry;
    this.instanceId = Objects.requireNonNull(instanceId);
  }

  @Override
  public RelayResult relayBatch() {
    List<OutboxEvent> events =
        Observation.createNotStarted("outbox.claim", observationRegistry)
            .observe(() -> repository.claimBatch(properties.batchSize(), instanceId));

    if (events == null || events.isEmpty()) {
      return new RelayResult(0, 0, 0, 0, 0);
    }

    metrics.incrementClaimed(events.size());

    int published = 0;
    int rescheduled = 0;
    int failed = 0;
    int lockLost = 0;

    for (OutboxEvent event : events) {
      Outcome outcome;
      try {
        PublicationResult result =
            Observation.createNotStarted("outbox.publish", observationRegistry)
                .lowCardinalityKeyValue("destination", event.destination())
                .lowCardinalityKeyValue("event_type", event.eventType())
                .observe(
                    () ->
                        metrics.recordPublication(
                            event.destination(),
                            event.eventType(),
                            () -> brokerPublisher.publish(event)));

        outcome = recordPublication(event, result);
      } catch (Exception exception) {
        outcome = handleFailure(event, exception);
      }

      switch (outcome) {
        case PUBLISHED -> published++;
        case RESCHEDULED -> rescheduled++;
        case FAILED -> failed++;
        case LOCK_LOST -> lockLost++;
      }
    }

    return new RelayResult(events.size(), published, rescheduled, failed, lockLost);
  }

  private Outcome recordPublication(OutboxEvent event, PublicationResult result) {
    Boolean owned =
        Observation.createNotStarted("outbox.mark-published", observationRegistry)
            .observe(
                () ->
                    repository.markPublished(
                        event.id(), instanceId, result.publishedAt(), result.brokerMessageId()));

    if (Boolean.FALSE.equals(owned)) {
      return lockLost(event, "publication succeeded but the outcome could not be recorded");
    }

    metrics.incrementPublished(event.destination(), event.eventType());
    return Outcome.PUBLISHED;
  }

  private Outcome handleFailure(OutboxEvent event, Exception exception) {
    RetryDecision decision = retryPolicy.evaluate(event, exception);
    int attempts = event.attempts() + 1;

    if (decision.retry()) {
      if (!repository.reschedule(
          event.id(), instanceId, attempts, decision.nextAttemptAt(), decision.reason())) {
        return lockLost(event, "retry could not be scheduled");
      }
      metrics.incrementRescheduled(event.destination(), event.eventType());
      log.warn(
          "Rescheduled outbox event {} to {} after failure: {}",
          event.id(),
          decision.nextAttemptAt(),
          decision.reason());
      return Outcome.RESCHEDULED;
    }

    if (!repository.markFailed(event.id(), instanceId, attempts, decision.reason())) {
      return lockLost(event, "failure could not be recorded");
    }
    metrics.incrementFailed(event.destination(), event.eventType());
    if (exception instanceof PermanentPublicationException
        || exception instanceof RetryablePublicationException) {
      log.error("Marked outbox event {} as FAILED: {}", event.id(), decision.reason());
    } else {
      log.error("Marked outbox event {} as FAILED: {}", event.id(), decision.reason(), exception);
    }
    return Outcome.FAILED;
  }

  /**
   * The claim on this row expired and another instance took it over, so the row is no longer ours
   * to write. Leaving it untouched is the only safe option: overwriting would clobber whatever the
   * new owner has already decided. If the publication itself had succeeded, the new owner will
   * publish it again, which is the at-least-once behaviour consumers already deduplicate against.
   *
   * <p>A steady stream of these means the relay is losing claims faster than it can finish them —
   * raise {@code lock-timeout}, or lower {@code batch-size} so batches complete inside it.
   */
  private Outcome lockLost(OutboxEvent event, String context) {
    metrics.incrementLockLost(event.destination(), event.eventType());
    log.warn(
        "Lost the claim on outbox event {} before it could be settled ({}); "
            + "instance {} no longer owns the row and left it untouched",
        event.id(),
        context,
        instanceId);
    return Outcome.LOCK_LOST;
  }

  private enum Outcome {
    PUBLISHED,
    RESCHEDULED,
    FAILED,
    LOCK_LOST
  }
}
