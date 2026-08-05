package io.github.erick9125.outbox.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public final class DefaultOutboxPublisher implements OutboxPublisher {

  private final OutboxRepository repository;
  private final ObjectMapper objectMapper;
  private final OutboxMetrics metrics;
  private final ObservationRegistry observationRegistry;
  private final Clock clock;

  public DefaultOutboxPublisher(
      OutboxRepository repository,
      ObjectMapper objectMapper,
      OutboxMetrics metrics,
      ObservationRegistry observationRegistry) {
    this(repository, objectMapper, metrics, observationRegistry, Clock.systemUTC());
  }

  public DefaultOutboxPublisher(
      OutboxRepository repository,
      ObjectMapper objectMapper,
      OutboxMetrics metrics,
      ObservationRegistry observationRegistry,
      Clock clock) {
    this.repository = Objects.requireNonNull(repository);
    this.objectMapper = Objects.requireNonNull(objectMapper);
    this.metrics = Objects.requireNonNull(metrics);
    this.observationRegistry =
        observationRegistry == null ? ObservationRegistry.NOOP : observationRegistry;
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public UUID publish(OutboxMessage message) {
    requireActiveTransaction(message);
    return Observation.createNotStarted("outbox.persist", observationRegistry)
        .lowCardinalityKeyValue("destination", message.destination())
        .lowCardinalityKeyValue("event_type", message.eventType())
        .observe(
            () -> {
              Instant occurredAt = clock.instant();
              String payloadJson = writeJson(message.payload(), "payload");
              String headersJson = writeJson(safeHeaders(message.headers()), "headers");
              UUID id = repository.insert(message, payloadJson, headersJson, occurredAt);
              metrics.incrementCreated(message.destination(), message.eventType());
              return id;
            });
  }

  /**
   * The whole point of the outbox pattern is that the event and the business change commit or roll
   * back together. Called outside a transaction the insert commits on its own, and the guarantee is
   * gone — silently, and in a way that only shows up as drift between the database and the broker
   * long after the fact. Failing loudly here turns the most common integration mistake into an
   * immediate, obvious error.
   */
  private static void requireActiveTransaction(OutboxMessage message) {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(
          "OutboxPublisher.publish() requires an active transaction, but none was found while "
              + "publishing event type '"
              + message.eventType()
              + "' for aggregate "
              + message.aggregateType()
              + "/"
              + message.aggregateId()
              + ". Call it from inside the @Transactional method that persists the business "
              + "change, so the outbox row and that change commit or roll back together.");
    }
  }

  private String writeJson(Object value, String fieldName) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new PermanentPublicationException("Failed to serialize outbox " + fieldName, exception);
    }
  }

  private static Map<String, String> safeHeaders(Map<String, String> headers) {
    return headers == null ? Map.of() : headers;
  }
}
