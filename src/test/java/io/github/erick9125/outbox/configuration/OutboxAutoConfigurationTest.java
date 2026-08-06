package io.github.erick9125.outbox.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxPublisher;
import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.persistence.JdbcOutboxRepository;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.github.erick9125.outbox.relay.OutboxRelay;
import io.github.erick9125.outbox.scheduling.OutboxScheduler;
import io.micrometer.observation.ObservationRegistry;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Exercises the library the way a consuming application sees it: through the auto-configuration and
 * Spring Boot's real {@code AutoConfigurationSorter}, rather than by wiring beans by hand.
 *
 * <p>{@link AutoConfigurations#of} applies the same ordering rules as a running application, which
 * matters because auto-configuration classes are sorted alphabetically before order annotations are
 * applied. A plain {@code withBean(KafkaTemplate.class, ...)} would register the bean up front and
 * hide ordering defects entirely.
 *
 * <p>No test here touches a database. The {@link StubOutboxRepository} keeps the scheduled relay
 * harmless; {@code JdbcOutboxRepository} itself is covered by the Testcontainers integration tests.
 */
class OutboxAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  JdbcTemplateAutoConfiguration.class,
                  JacksonAutoConfiguration.class,
                  KafkaAutoConfiguration.class,
                  OutboxAutoConfiguration.class,
                  OutboxSchedulingAutoConfiguration.class))
          .withBean(
              DataSource.class,
              () -> new DriverManagerDataSource("jdbc:postgresql://localhost:5432/unused"));

  @Test
  void registersRelayAndSchedulerWhenKafkaTemplateIsPresent() {
    runner
        .withBean(OutboxRepository.class, StubOutboxRepository::new)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(KafkaTemplate.class);
              assertThat(context).hasSingleBean(MessageBrokerPublisher.class);
              assertThat(context).hasSingleBean(OutboxRelay.class);
              assertThat(context).hasSingleBean(OutboxScheduler.class);
              assertThat(context).hasSingleBean(OutboxPublisher.class);
            });
  }

  @Test
  void startsWithoutAnObjectMapperBean() {
    // Spring Boot only contributes an ObjectMapper when Jackson2ObjectMapperBuilder is on the
    // classpath, and that class ships with spring-web. A headless relay has neither, so the
    // auto-configuration must fall back to its own mapper instead of failing to start.
    runner
        .withBean(OutboxRepository.class, StubOutboxRepository::new)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean(ObjectMapper.class);
              assertThat(context).hasSingleBean(OutboxPublisher.class);
            });
  }

  @Test
  void doesNotOverrideAnApplicationSuppliedObservationRegistry() {
    // The library must never contribute an ObservationRegistry of its own: it would be registered
    // before Spring Boot's, suppressing the host application's observation handlers and tracing.
    ObservationRegistry applicationRegistry = ObservationRegistry.create();

    runner
        .withBean(OutboxRepository.class, StubOutboxRepository::new)
        .withBean(ObservationRegistry.class, () -> applicationRegistry)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(ObservationRegistry.class);
              assertThat(context.getBean(ObservationRegistry.class)).isSameAs(applicationRegistry);
            });
  }

  @Test
  void contributesNoObservationRegistryOfItsOwn() {
    runner
        .withBean(OutboxRepository.class, StubOutboxRepository::new)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean(ObservationRegistry.class);
            });
  }

  @Test
  void skipsRelayButStaysHealthyWithoutAMessageBrokerPublisher() {
    // No spring.kafka.bootstrap-servers and no broker adapter: the publisher side must still work
    // so events accumulate durably, and the context must not fail.
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                JdbcTemplateAutoConfiguration.class,
                JacksonAutoConfiguration.class,
                OutboxAutoConfiguration.class,
                OutboxSchedulingAutoConfiguration.class))
        .withBean(
            DataSource.class,
            () -> new DriverManagerDataSource("jdbc:postgresql://localhost:5432/unused"))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(OutboxPublisher.class);
              assertThat(context).hasSingleBean(JdbcOutboxRepository.class);
              assertThat(context).doesNotHaveBean(MessageBrokerPublisher.class);
              assertThat(context).doesNotHaveBean(OutboxRelay.class);
              assertThat(context).doesNotHaveBean(OutboxScheduler.class);
            });
  }

  @Test
  void yieldsToAnApplicationSuppliedMessageBrokerPublisher() {
    MessageBrokerPublisher custom =
        event -> CompletableFuture.completedFuture(new PublicationResult("custom", Instant.EPOCH));

    runner
        .withBean(OutboxRepository.class, StubOutboxRepository::new)
        .withBean(MessageBrokerPublisher.class, () -> custom)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(MessageBrokerPublisher.class);
              assertThat(context.getBean(MessageBrokerPublisher.class)).isSameAs(custom);
              assertThat(context).hasSingleBean(OutboxRelay.class);
            });
  }

  @Test
  void backsOffEntirelyWhenDisabled() {
    runner
        .withPropertyValues(
            "spring.outbox.relay.enabled=false", "spring.kafka.bootstrap-servers=localhost:9092")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean(OutboxPublisher.class);
              assertThat(context).doesNotHaveBean(OutboxRepository.class);
              assertThat(context).doesNotHaveBean(OutboxRelay.class);
              assertThat(context).doesNotHaveBean(OutboxScheduler.class);
            });
  }

  @Test
  void backsOffWithoutADataSource() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean(OutboxPublisher.class);
              assertThat(context).doesNotHaveBean(OutboxRepository.class);
            });
  }

  @Test
  void registersEveryAutoConfigurationInTheImportsFile() throws Exception {
    // An auto-configuration class missing from this file is silently inert in a real application,
    // and no context test above would catch it because they name their classes explicitly.
    try (InputStream stream =
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")) {
      assertThat(stream).isNotNull();
      List<String> imports =
          new String(stream.readAllBytes(), StandardCharsets.UTF_8)
              .lines()
              .map(String::trim)
              .filter(line -> !line.isEmpty())
              .toList();

      assertThat(imports)
          .containsExactlyInAnyOrder(
              OutboxAutoConfiguration.class.getName(),
              OutboxSchedulingAutoConfiguration.class.getName());
    }
  }

  /** Keeps the scheduled relay from reaching a database during auto-configuration tests. */
  private static final class StubOutboxRepository implements OutboxRepository {

    @Override
    public UUID insert(
        OutboxMessage message, String payloadJson, String headersJson, Instant occurredAt) {
      return UUID.randomUUID();
    }

    @Override
    public List<OutboxEvent> claimBatch(int batchSize, String lockedBy) {
      return List.of();
    }

    @Override
    public boolean markPublished(
        UUID id, String lockedBy, Instant publishedAt, String brokerMessageId) {
      return true;
    }

    @Override
    public boolean reschedule(
        UUID id, String lockedBy, int attempts, Instant availableAt, String lastError) {
      return true;
    }

    @Override
    public boolean markFailed(UUID id, String lockedBy, int attempts, String lastError) {
      return true;
    }

    @Override
    public int recoverAbandoned(Instant lockedBefore, int maxRecoveries, int limit) {
      return 0;
    }

    @Override
    public int deletePublishedBefore(Instant publishedBefore, int limit) {
      return 0;
    }

    @Override
    public int failExhaustedRecoveries(Instant lockedBefore, int maxRecoveries, int limit) {
      return 0;
    }

    @Override
    public long countPending() {
      return 0L;
    }

    @Override
    public Optional<Instant> findOldestPendingAvailableAt() {
      return Optional.empty();
    }

    @Override
    public Optional<OutboxEvent> findById(UUID id) {
      return Optional.empty();
    }
  }
}
