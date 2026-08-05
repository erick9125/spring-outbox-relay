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
      return new RelayResult(0, 0, 0, 0);
    }

    metrics.incrementClaimed(events.size());

    int published = 0;
    int rescheduled = 0;
    int failed = 0;

    for (OutboxEvent event : events) {
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

        Observation.createNotStarted("outbox.mark-published", observationRegistry)
            .observe(
                () -> {
                  repository.markPublished(
                      event.id(), result.publishedAt(), result.brokerMessageId());
                  return null;
                });
        metrics.incrementPublished(event.destination(), event.eventType());
        published++;
      } catch (Exception exception) {
        if (handleFailure(event, exception)) {
          rescheduled++;
        } else {
          failed++;
        }
      }
    }

    return new RelayResult(events.size(), published, rescheduled, failed);
  }

  private boolean handleFailure(OutboxEvent event, Exception exception) {
    RetryDecision decision = retryPolicy.evaluate(event, exception);
    int attempts = event.attempts() + 1;

    if (decision.retry()) {
      repository.reschedule(event.id(), attempts, decision.nextAttemptAt(), decision.reason());
      metrics.incrementRescheduled(event.destination(), event.eventType());
      log.warn(
          "Rescheduled outbox event {} to {} after failure: {}",
          event.id(),
          decision.nextAttemptAt(),
          decision.reason());
      return true;
    }

    repository.markFailed(event.id(), attempts, decision.reason());
    metrics.incrementFailed(event.destination(), event.eventType());
    if (exception instanceof PermanentPublicationException
        || exception instanceof RetryablePublicationException) {
      log.error("Marked outbox event {} as FAILED: {}", event.id(), decision.reason());
    } else {
      log.error("Marked outbox event {} as FAILED: {}", event.id(), decision.reason(), exception);
    }
    return false;
  }
}
