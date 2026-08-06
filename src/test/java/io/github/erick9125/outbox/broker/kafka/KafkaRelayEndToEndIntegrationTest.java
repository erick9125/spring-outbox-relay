package io.github.erick9125.outbox.broker.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.RelayResult;
import io.github.erick9125.outbox.relay.OutboxRelay;
import io.github.erick9125.outbox.support.AbstractPostgresIntegrationTest;
import io.github.erick9125.outbox.support.OutboxTestSupport;
import io.github.erick9125.outbox.support.OutboxTestSupport.Fixture;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The only test that puts a real broker behind the relay.
 *
 * <p>Everything else about the Kafka path is verified structurally — the adapter's unit tests use a
 * {@code KafkaTemplate} that captures records in memory, and the relay's tests use fake publishers.
 * That leaves the parts that only a broker can prove: that the payload survives serialization, that
 * the headers arrive as a consumer sees them, that the record lands under the partition key, and
 * that the producer's future resolves to real metadata the row can record.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaRelayEndToEndIntegrationTest extends AbstractPostgresIntegrationTest {

  private static final String TOPIC = "orders.events.e2e";

  @Container private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

  private Fixture fixture;
  private KafkaTemplate<String, String> kafkaTemplate;

  @BeforeEach
  void setUp() throws Exception {
    fixture = OutboxTestSupport.fixture(POSTGRES);
    fixture.jdbcTemplate().update("DELETE FROM outbox_event");
    createTopic();
    kafkaTemplate =
        new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(
                Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class,
                    ProducerConfig.ACKS_CONFIG,
                    "all")));
  }

  @Test
  void relaysAnEventToARealBrokerWithEveryHeaderIntact() {
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("order-77")
                .eventType("order.created")
                .eventVersion(3)
                .destination(TOPIC)
                .partitionKey("order-77")
                .payload(Map.of("total", 4990, "currency", "CLP"))
                .header("correlation-id", "corr-abc")
                .header("tenant", "acme")
                .build());

    OutboxRelay relay =
        OutboxTestSupport.relay(fixture, new KafkaOutboxPublisher(kafkaTemplate), "e2e-worker");

    RelayResult result = relay.relayBatch();

    assertThat(result.published()).isEqualTo(1);
    assertThat(result.failed()).isZero();
    assertThat(result.rescheduled()).isZero();
    assertThat(result.lockLost()).isZero();

    ConsumerRecord<String, String> record = consumeOne();

    // The key decides the partition, so it has to be the partition key and not the event id.
    assertThat(record.key()).isEqualTo("order-77");
    assertThat(record.value()).contains("4990").contains("CLP");

    Map<String, String> headers = headersOf(record);
    assertThat(headers)
        .containsEntry("event-id", id.toString())
        .containsEntry("event-type", "order.created")
        .containsEntry("event-version", "3")
        .containsEntry("aggregate-type", "ORDER")
        .containsEntry("aggregate-id", "order-77")
        // The headers the caller attached: persisted as JSONB and read back out for the record.
        .containsEntry("correlation-id", "corr-abc")
        .containsEntry("tenant", "acme");

    // The row records what the broker actually assigned, from the producer's own future.
    OutboxEvent event = fixture.repository().findById(id).orElseThrow();
    assertThat(event.status()).isEqualTo(OutboxStatus.PUBLISHED);
    assertThat(event.publishedAt()).isNotNull();
    assertThat(event.headers())
        .contains("brokerMessageId")
        .contains(TOPIC + "-" + record.partition() + "-" + record.offset());
  }

  @Test
  void marksTheEventFailedWhenTheTopicNameIsRejectedByTheBroker() {
    // A destination the broker refuses is permanent: retrying cannot help, so the row must end up
    // FAILED and inspectable rather than cycling until its budget runs out.
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("order-bad")
                .eventType("order.created")
                .destination("not a valid topic name")
                .payload(Map.of("ok", true))
                .build());

    OutboxRelay relay =
        OutboxTestSupport.relay(fixture, new KafkaOutboxPublisher(kafkaTemplate), "e2e-worker");

    RelayResult result = relay.relayBatch();

    assertThat(result.failed()).isEqualTo(1);
    assertThat(result.published()).isZero();

    OutboxEvent event = fixture.repository().findById(id).orElseThrow();
    assertThat(event.status()).isEqualTo(OutboxStatus.FAILED);
    assertThat(event.lastError()).isNotBlank();
  }

  private void createTopic() throws InterruptedException, ExecutionException {
    try (AdminClient admin =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      if (admin.listTopics().names().get().contains(TOPIC)) {
        return;
      }
      admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
    }
  }

  private ConsumerRecord<String, String> consumeOne() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-" + UUID.randomUUID());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(List.of(TOPIC));

      // The first polls return nothing while the consumer joins the group, so this waits on a
      // deadline rather than trusting a single call.
      Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
      List<ConsumerRecord<String, String>> received = new ArrayList<>();
      while (received.isEmpty() && Instant.now().isBefore(deadline)) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        records.records(TOPIC).forEach(received::add);
      }

      assertThat(received).as("no record arrived on %s within 30s", TOPIC).hasSize(1);
      return received.get(0);
    }
  }

  private static Map<String, String> headersOf(ConsumerRecord<String, String> record) {
    Map<String, String> headers = new HashMap<>();
    for (Header header : record.headers()) {
      headers.put(header.key(), new String(header.value(), UTF_8));
    }
    return headers;
  }
}
