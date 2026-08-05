package io.github.erick9125.outbox.broker.kafka;

import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.exception.RetryablePublicationException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

public final class KafkaOutboxPublisher implements MessageBrokerPublisher {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final Clock clock;
  private final long sendTimeoutSeconds;

  public KafkaOutboxPublisher(KafkaTemplate<String, String> kafkaTemplate) {
    this(kafkaTemplate, Clock.systemUTC(), 30L);
  }

  public KafkaOutboxPublisher(
      KafkaTemplate<String, String> kafkaTemplate, Clock clock, long sendTimeoutSeconds) {
    this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.sendTimeoutSeconds = sendTimeoutSeconds;
  }

  @Override
  public PublicationResult publish(OutboxEvent event) {
    ProducerRecord<String, String> record =
        new ProducerRecord<>(event.destination(), event.partitionKey(), event.payload());

    addHeader(record, "event-id", event.id().toString());
    addHeader(record, "event-type", event.eventType());
    addHeader(record, "event-version", Integer.toString(event.eventVersion()));
    addHeader(record, "aggregate-type", event.aggregateType());
    addHeader(record, "aggregate-id", event.aggregateId());

    try {
      SendResult<String, String> result =
          kafkaTemplate.send(record).get(sendTimeoutSeconds, TimeUnit.SECONDS);
      String brokerMessageId =
          result.getRecordMetadata().topic()
              + "-"
              + result.getRecordMetadata().partition()
              + "-"
              + result.getRecordMetadata().offset();
      return new PublicationResult(brokerMessageId, clock.instant());
    } catch (TimeoutException exception) {
      throw new RetryablePublicationException("Timed out waiting for Kafka ack", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new RetryablePublicationException("Interrupted while publishing to Kafka", exception);
    } catch (ExecutionException exception) {
      throw mapExecutionFailure(exception);
    }
  }

  private static RuntimeException mapExecutionFailure(ExecutionException exception) {
    Throwable cause = exception.getCause() == null ? exception : exception.getCause();
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
