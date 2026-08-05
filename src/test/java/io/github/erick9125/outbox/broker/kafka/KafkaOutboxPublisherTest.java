package io.github.erick9125.outbox.broker.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Covers the Kafka adapter's record mapping without a broker. {@link KafkaTemplate} is subclassed
 * so that {@code send} captures the record and acknowledges it immediately, which keeps the
 * assertions about headers and keys focused and fast.
 */
class KafkaOutboxPublisherTest {

  private final List<ProducerRecord<String, String>> sent = new ArrayList<>();
  private final KafkaOutboxPublisher publisher =
      new KafkaOutboxPublisher(
          new CapturingKafkaTemplate(sent), Clock.fixed(Instant.EPOCH, ZoneOffset.UTC), 5L);

  @Test
  void propagatesTheHeadersTheCallerAttachedToTheMessage() {
    publisher.publish(event("{\"correlation-id\":\"abc-123\",\"tenant\":\"acme\"}"));

    assertThat(headersOf(sent.get(0)))
        .containsEntry("correlation-id", "abc-123")
        .containsEntry("tenant", "acme");
  }

  @Test
  void alwaysWritesTheRelayMetadataHeaders() {
    OutboxEvent event = event("{}");

    publisher.publish(event);

    assertThat(headersOf(sent.get(0)))
        .containsEntry("event-id", event.id().toString())
        .containsEntry("event-type", "order.created")
        .containsEntry("event-version", "3")
        .containsEntry("aggregate-type", "ORDER")
        .containsEntry("aggregate-id", "order-1");
  }

  @Test
  void doesNotLetCallerHeadersShadowTheRelayMetadata() {
    // Kafka headers are multi-valued, so a caller-supplied event-id would otherwise be delivered
    // alongside the real one and consumers could deduplicate against the wrong value.
    OutboxEvent event = event("{\"event-id\":\"spoofed\",\"event-type\":\"spoofed\"}");

    publisher.publish(event);

    assertThat(valuesOf(sent.get(0), "event-id")).containsExactly(event.id().toString());
    assertThat(valuesOf(sent.get(0), "event-type")).containsExactly("order.created");
  }

  @Test
  void toleratesAbsentOrEmptyHeaders() {
    publisher.publish(event(null));
    publisher.publish(event("   "));
    publisher.publish(event("{}"));

    assertThat(sent).hasSize(3);
    assertThat(headersOf(sent.get(0))).containsKey("event-id");
  }

  @Test
  void failsPermanentlyWhenTheHeadersColumnIsNotAJsonObject() {
    // Only reachable if something other than this library wrote the column. A permanent failure
    // puts the row in FAILED with last_error rather than retrying what cannot succeed.
    assertThatThrownBy(() -> publisher.publish(event("not json at all")))
        .isInstanceOf(PermanentPublicationException.class)
        .hasMessageContaining("not a flat JSON object");

    assertThat(sent).isEmpty();
  }

  @Test
  void usesThePartitionKeyAsTheRecordKeyAndReportsTheBrokerOffset() {
    PublicationResult result = publisher.publish(event("{}"));

    assertThat(sent.get(0).key()).isEqualTo("order-1");
    assertThat(sent.get(0).topic()).isEqualTo("orders.events");
    assertThat(result.brokerMessageId()).isEqualTo("orders.events-2-77");
    assertThat(result.publishedAt()).isEqualTo(Instant.EPOCH);
  }

  private static OutboxEvent event(String headersJson) {
    return new OutboxEvent(
        UUID.randomUUID(),
        "ORDER",
        "order-1",
        "order.created",
        3,
        "orders.events",
        "order-1",
        "{\"total\":10}",
        headersJson,
        OutboxStatus.PROCESSING,
        0,
        5,
        Instant.EPOCH,
        Instant.EPOCH,
        Instant.EPOCH,
        Instant.EPOCH,
        "worker-a",
        null,
        null);
  }

  private static Map<String, String> headersOf(ProducerRecord<String, String> record) {
    Map<String, String> headers = new LinkedHashMap<>();
    for (Header header : record.headers()) {
      headers.put(header.key(), new String(header.value(), UTF_8));
    }
    return headers;
  }

  private static List<String> valuesOf(ProducerRecord<String, String> record, String key) {
    List<String> values = new ArrayList<>();
    for (Header header : record.headers().headers(key)) {
      values.add(new String(header.value(), UTF_8));
    }
    return values;
  }

  /** Captures records instead of talking to a broker, and acknowledges them immediately. */
  private static final class CapturingKafkaTemplate extends KafkaTemplate<String, String> {

    private final List<ProducerRecord<String, String>> captured;

    private CapturingKafkaTemplate(List<ProducerRecord<String, String>> captured) {
      super(new DefaultKafkaProducerFactory<>(Map.of()));
      this.captured = captured;
    }

    @Override
    public CompletableFuture<SendResult<String, String>> send(
        ProducerRecord<String, String> record) {
      captured.add(record);
      RecordMetadata metadata =
          new RecordMetadata(new TopicPartition(record.topic(), 2), 77L, 0, 0L, 0, 0);
      return CompletableFuture.completedFuture(new SendResult<>(record, metadata));
    }
  }
}
