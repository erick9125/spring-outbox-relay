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
