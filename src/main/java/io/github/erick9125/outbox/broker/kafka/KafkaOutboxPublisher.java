package io.github.erick9125.outbox.broker.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.exception.RetryablePublicationException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

public final class KafkaOutboxPublisher implements MessageBrokerPublisher {

  private static final Logger log = LoggerFactory.getLogger(KafkaOutboxPublisher.class);

  /** Header names the relay owns. Callers cannot set or shadow these. */
  private static final Set<String> RESERVED_HEADERS =
      Set.of("event-id", "event-type", "event-version", "aggregate-type", "aggregate-id");

  private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {};

  /**
   * Reads back the {@code headers} column, whose shape this library controls end to end: a flat
   * JSON object of strings written by {@code DefaultOutboxPublisher}. It is deliberately not the
   * application's {@code ObjectMapper} — a customised one could change how that internal format is
   * read.
   */
  private static final ObjectMapper HEADER_MAPPER = new ObjectMapper();

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final Clock clock;

  public KafkaOutboxPublisher(KafkaTemplate<String, String> kafkaTemplate) {
    this(kafkaTemplate, Clock.systemUTC());
  }

  public KafkaOutboxPublisher(KafkaTemplate<String, String> kafkaTemplate, Clock clock) {
    this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  /**
   * Hands the record to the producer and maps its future, without waiting for the acknowledgement.
   *
   * <p>The producer batches concurrent sends itself, so handing off the whole outbox batch before
   * awaiting any of it is what lets one poll cost roughly one round trip instead of one per event.
   * The relay owns the deadline; this adapter only translates Kafka's failures into the retryable
   * and permanent categories the retry policy understands.
   */
  @Override
  public CompletableFuture<PublicationResult> publish(OutboxEvent event) {
    ProducerRecord<String, String> record =
        new ProducerRecord<>(event.destination(), event.partitionKey(), event.payload());

    addUserHeaders(record, event);

    addHeader(record, "event-id", event.id().toString());
    addHeader(record, "event-type", event.eventType());
    addHeader(record, "event-version", Integer.toString(event.eventVersion()));
    addHeader(record, "aggregate-type", event.aggregateType());
    addHeader(record, "aggregate-id", event.aggregateId());

    CompletableFuture<SendResult<String, String>> sent;
    try {
      sent = kafkaTemplate.send(record);
    } catch (RuntimeException exception) {
      // send() itself can fail before anything is queued — a closed producer, or a full buffer once
      // max.block.ms elapses.
      return CompletableFuture.failedFuture(mapFailure(exception));
    }

    return sent.handle(
        (result, failure) -> {
          if (failure != null) {
            throw mapFailure(failure);
          }
          return new PublicationResult(brokerMessageId(result), clock.instant());
        });
  }

  private static String brokerMessageId(SendResult<String, String> result) {
    return result.getRecordMetadata().topic()
        + "-"
        + result.getRecordMetadata().partition()
        + "-"
        + result.getRecordMetadata().offset();
  }

  /**
   * Copies the headers the caller attached to the {@code OutboxMessage} onto the record.
   *
   * <p>Names that clash with the relay's own metadata are dropped rather than added alongside it:
   * Kafka headers are multi-valued, so appending a second {@code event-id} would leave consumers
   * deduplicating against whichever one they happened to read first.
   */
  private static void addUserHeaders(ProducerRecord<String, String> record, OutboxEvent event) {
    for (Map.Entry<String, String> header : readHeaders(event).entrySet()) {
      if (RESERVED_HEADERS.contains(header.getKey())) {
        log.warn(
            "Dropping header '{}' on outbox event {}: the name is reserved for relay metadata",
            header.getKey(),
            event.id());
        continue;
      }
      addHeader(record, header.getKey(), header.getValue());
    }
  }

  private static Map<String, String> readHeaders(OutboxEvent event) {
    String headers = event.headers();
    if (headers == null || headers.isBlank()) {
      return Map.of();
    }
    try {
      Map<String, String> parsed = HEADER_MAPPER.readValue(headers, HEADERS_TYPE);
      return parsed == null ? Map.of() : parsed;
    } catch (JsonProcessingException exception) {
      // This library writes the column, so unreadable content means the row was changed by
      // something else. Failing permanently puts it in FAILED with last_error for an operator to
      // look at, instead of retrying what cannot succeed or dropping the headers unnoticed.
      throw new PermanentPublicationException(
          "Outbox event " + event.id() + " has headers that are not a flat JSON object", exception);
    }
  }

  /**
   * Classifies a producer failure. Unwraps the {@code CompletionException} and {@code
   * KafkaProducerException} layers the client adds so the decision is made on the real cause.
   */
  private static RuntimeException mapFailure(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof PermanentPublicationException permanent) {
      return permanent;
    }
    if (isPermanent(cause)) {
      return new PermanentPublicationException("Permanent Kafka publication failure", cause);
    }
    return new RetryablePublicationException("Retryable Kafka publication failure", cause);
  }

  private static boolean isPermanent(Throwable cause) {
    return cause instanceof InvalidTopicException
        || cause instanceof SerializationException
        || cause instanceof RecordTooLargeException
        || cause instanceof IllegalArgumentException;
  }

  private static void addHeader(ProducerRecord<String, String> record, String key, String value) {
    if (value != null) {
      record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
