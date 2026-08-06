package io.github.erick9125.outbox.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.erick9125.outbox.api.DefaultOutboxPublisher;
import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxPublisher;
import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.persistence.JdbcOutboxRepository;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.github.erick9125.outbox.relay.DefaultOutboxRelay;
import io.github.erick9125.outbox.relay.OutboxRelay;
import io.github.erick9125.outbox.retry.ExponentialBackoffRetryPolicy;
import io.github.erick9125.outbox.retry.RetryPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

public final class OutboxTestSupport {

  private OutboxTestSupport() {}

  public static DataSource dataSource(PostgreSQLContainer<?> postgres) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(postgres.getDriverClassName());
    dataSource.setUrl(postgres.getJdbcUrl());
    dataSource.setUsername(postgres.getUsername());
    dataSource.setPassword(postgres.getPassword());
    return dataSource;
  }

  public static void migrate(DataSource dataSource) {
    Flyway.configure().dataSource(dataSource).locations("classpath:db/outbox").load().migrate();
  }

  public static ObjectMapper objectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  public static OutboxProperties properties(int batchSize, int maxAttempts) {
    return new OutboxProperties(
        true,
        batchSize,
        Duration.ofMillis(100),
        Duration.ofMinutes(5),
        maxAttempts,
        "test-instance",
        Duration.ofSeconds(30),
        500,
        Duration.ofSeconds(30),
        Duration.ofSeconds(10),
        Duration.ofSeconds(30),
        new OutboxProperties.Retry(Duration.ofSeconds(5), Duration.ofMinutes(5), 2.0d, 0.0d),
        OutboxProperties.Cleanup.defaults());
  }

  public static Fixture fixture(PostgreSQLContainer<?> postgres) {
    DataSource dataSource = dataSource(postgres);
    migrate(dataSource);
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    OutboxProperties properties = properties(50, 5);
    OutboxRepository repository = new JdbcOutboxRepository(jdbcTemplate, properties);
    OutboxMetrics metrics = new OutboxMetrics(new SimpleMeterRegistry(), repository);
    OutboxPublisher publisher =
        new DefaultOutboxPublisher(repository, objectMapper(), metrics, ObservationRegistry.NOOP);
    PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
    return new Fixture(
        dataSource,
        jdbcTemplate,
        new TransactionTemplate(txManager),
        properties,
        repository,
        publisher,
        metrics);
  }

  /**
   * Wraps a synchronous publisher as the async {@link MessageBrokerPublisher} the relay expects.
   * Throwing from {@code publisher} still surfaces as a failed publication, because the relay
   * treats an adapter that throws the same as one that returns a failed future.
   */
  public static MessageBrokerPublisher sync(Function<OutboxEvent, PublicationResult> publisher) {
    return event -> CompletableFuture.completedFuture(publisher.apply(event));
  }

  public static OutboxRelay relay(
      Fixture fixture, MessageBrokerPublisher brokerPublisher, String instanceId) {
    return relayWithPublishTimeout(
        fixture, brokerPublisher, instanceId, fixture.properties().publishTimeout());
  }

  public static OutboxRelay relayWithPublishTimeout(
      Fixture fixture,
      MessageBrokerPublisher brokerPublisher,
      String instanceId,
      Duration publishTimeout) {
    RetryPolicy retryPolicy =
        new ExponentialBackoffRetryPolicy(
            Duration.ofSeconds(5), Duration.ofMinutes(5), 2.0d, 0.0d, Clock.systemUTC());
    OutboxProperties properties =
        new OutboxProperties(
            true,
            fixture.properties().batchSize(),
            fixture.properties().pollInterval(),
            fixture.properties().lockTimeout(),
            fixture.properties().defaultMaxAttempts(),
            instanceId,
            fixture.properties().recoveryInterval(),
            fixture.properties().maintenanceBatchSize(),
            publishTimeout,
            fixture.properties().backlogMetricsInterval(),
            fixture.properties().shutdownTimeout(),
            fixture.properties().retry(),
            fixture.properties().cleanup());
    return new DefaultOutboxRelay(
        fixture.repository(),
        brokerPublisher,
        retryPolicy,
        properties,
        fixture.metrics(),
        ObservationRegistry.NOOP,
        instanceId);
  }

  public record Fixture(
      DataSource dataSource,
      JdbcTemplate jdbcTemplate,
      TransactionTemplate transactionTemplate,
      OutboxProperties properties,
      OutboxRepository repository,
      OutboxPublisher publisher,
      OutboxMetrics metrics) {

    /**
     * Publishes inside a transaction, which is what {@code OutboxPublisher} requires. Propagation
     * is REQUIRED, so this joins an enclosing transaction when a test already opened one.
     */
    public UUID publish(OutboxMessage message) {
      return transactionTemplate.execute(status -> publisher.publish(message));
    }
  }
}
