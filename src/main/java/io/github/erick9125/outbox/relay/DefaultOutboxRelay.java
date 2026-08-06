package io.github.erick9125.outbox.relay;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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

    // Phase one: hand the whole batch to the broker. Nothing is awaited yet, so the batch costs one
    // round trip rather than one per event. Awaiting each acknowledgement before sending the next
    // made a batch take up to batch-size x publish-timeout, long enough for lock-timeout to elapse
    // and for the recovery job to reclaim rows that were still being published.
    List<InFlight> inFlight = events.stream().map(this::send).toList();

    // Phase two: settle each one against a single deadline for the whole batch.
    Instant deadline = Instant.now().plus(properties.publishTimeout());

    int published = 0;
    int rescheduled = 0;
    int failed = 0;
    int lockLost = 0;

    for (InFlight pending : inFlight) {
      switch (settle(pending, deadline)) {
        case PUBLISHED -> published++;
        case RESCHEDULED -> rescheduled++;
        case FAILED -> failed++;
        case LOCK_LOST -> lockLost++;
      }
    }

    return new RelayResult(events.size(), published, rescheduled, failed, lockLost);
  }

  /**
   * Hands one event to the broker. An adapter that throws instead of returning a failed future is
   * treated the same way, so a misbehaving adapter cannot abort the rest of the batch.
   */
  private InFlight send(OutboxEvent event) {
    Observation observation =
        Observation.createNotStarted("outbox.publish", observationRegistry)
            .lowCardinalityKeyValue("destination", event.destination())
            .lowCardinalityKeyValue("event_type", event.eventType())
            .start();

    long startedAt = System.nanoTime();
    CompletableFuture<PublicationResult> future;
    try {
      future = brokerPublisher.publish(event);
      if (future == null) {
        future =
            CompletableFuture.failedFuture(
                new IllegalStateException("MessageBrokerPublisher returned a null future"));
      }
    } catch (RuntimeException exception) {
      future = CompletableFuture.failedFuture(exception);
    }

    // Timed on completion rather than at settle time, so the recorded latency is the broker's own
    // and not inflated by the events settled ahead of this one.
    future.whenComplete(
        (result, failure) ->
            metrics.recordPublicationDuration(
                event.destination(),
                event.eventType(),
                failure == null,
                Duration.ofNanos(System.nanoTime() - startedAt)));

    return new InFlight(event, future, observation);
  }

  private Outcome settle(InFlight pending, Instant deadline) {
    try {
      PublicationResult result = pending.future().get(remainingMillis(deadline), MILLISECONDS);
      pending.observation().stop();
      return recordPublication(pending.event(), result);
    } catch (TimeoutException exception) {
      // The send may still land later, which is exactly the at-least-once window consumers
      // deduplicate against.
      return fail(
          pending,
          new RetryablePublicationException(
              "Broker did not acknowledge within " + properties.publishTimeout(), exception));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      return fail(
          pending,
          new RetryablePublicationException("Interrupted while awaiting the broker", exception));
    } catch (ExecutionException exception) {
      return fail(pending, unwrap(exception));
    } catch (RuntimeException exception) {
      return fail(pending, exception);
    }
  }

  private Outcome fail(InFlight pending, Throwable failure) {
    pending.observation().error(failure);
    pending.observation().stop();
    return handleFailure(pending.event(), failure);
  }

  private static long remainingMillis(Instant deadline) {
    long remaining = Duration.between(Instant.now(), deadline).toMillis();
    return Math.max(0L, remaining);
  }

  /** Strips the future plumbing so the retry policy and {@code last_error} see the real cause. */
  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof ExecutionException || cause instanceof CompletionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private record InFlight(
      OutboxEvent event, CompletableFuture<PublicationResult> future, Observation observation) {}

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

  private Outcome handleFailure(OutboxEvent event, Throwable exception) {
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
