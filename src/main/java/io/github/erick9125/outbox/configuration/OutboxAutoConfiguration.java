package io.github.erick9125.outbox.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.erick9125.outbox.api.DefaultOutboxPublisher;
import io.github.erick9125.outbox.api.OutboxPublisher;
import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.broker.kafka.KafkaOutboxPublisher;
import io.github.erick9125.outbox.cleanup.DefaultOutboxCleanupService;
import io.github.erick9125.outbox.cleanup.OutboxCleanupService;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.persistence.JdbcOutboxRepository;
import io.github.erick9125.outbox.persistence.OutboxRepository;
import io.github.erick9125.outbox.recovery.DefaultOutboxRecoveryService;
import io.github.erick9125.outbox.recovery.OutboxRecoveryService;
import io.github.erick9125.outbox.relay.DefaultOutboxRelay;
import io.github.erick9125.outbox.relay.OutboxRelay;
import io.github.erick9125.outbox.retry.ExponentialBackoffRetryPolicy;
import io.github.erick9125.outbox.retry.RetryPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.net.InetAddress;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Auto-configuration for the transactional outbox relay.
 *
 * <p>Must be processed after the auto-configurations that contribute the beans this library
 * conditionally depends on. Auto-configuration classes are sorted alphabetically before order
 * annotations are applied, so without the explicit {@code @AutoConfigureAfter} below this class
 * would be evaluated before {@code KafkaAutoConfiguration} and the {@code @ConditionalOnBean}
 * checks would silently fail, leaving the application with a publisher but no relay.
 *
 * <p>The auto-configuration class names are referenced by name rather than by type because {@code
 * MetricsAutoConfiguration} and {@code ObservationAutoConfiguration} live in
 * spring-boot-actuator-autoconfigure, which is not a dependency of this library.
 */
@AutoConfiguration
@AutoConfigureAfter(
    name = {
      "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
      "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
      "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration",
      "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
      "org.springframework.boot.actuate.autoconfigure.observation.ObservationAutoConfiguration"
    })
@EnableConfigurationProperties(OutboxProperties.class)
@ConditionalOnClass(JdbcTemplate.class)
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(
    prefix = "spring.outbox.relay",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@Import(OutboxAutoConfiguration.OutboxKafkaConfiguration.class)
public class OutboxAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  OutboxRepository outboxRepository(JdbcTemplate jdbcTemplate, OutboxProperties properties) {
    return new JdbcOutboxRepository(jdbcTemplate, properties);
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxMetrics outboxMetrics(
      ObjectProvider<MeterRegistry> meterRegistry, OutboxRepository outboxRepository) {
    return new OutboxMetrics(
        meterRegistry.getIfAvailable(SimpleMeterRegistry::new), outboxRepository);
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxPublisher outboxPublisher(
      OutboxRepository outboxRepository,
      ObjectProvider<ObjectMapper> objectMapper,
      OutboxMetrics outboxMetrics,
      ObjectProvider<ObservationRegistry> observationRegistry) {
    return new DefaultOutboxPublisher(
        outboxRepository,
        objectMapper.getIfAvailable(OutboxAutoConfiguration::defaultObjectMapper),
        outboxMetrics,
        resolveObservationRegistry(observationRegistry));
  }

  @Bean
  @ConditionalOnMissingBean
  RetryPolicy outboxRetryPolicy(OutboxProperties properties) {
    OutboxProperties.Retry retry = properties.retry();
    return new ExponentialBackoffRetryPolicy(
        retry.initialDelay(), retry.maximumDelay(), retry.multiplier(), retry.jitter());
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean(MessageBrokerPublisher.class)
  OutboxRelay outboxRelay(
      OutboxRepository outboxRepository,
      MessageBrokerPublisher messageBrokerPublisher,
      RetryPolicy retryPolicy,
      OutboxProperties properties,
      OutboxMetrics outboxMetrics,
      ObjectProvider<ObservationRegistry> observationRegistry) {
    return new DefaultOutboxRelay(
        outboxRepository,
        messageBrokerPublisher,
        retryPolicy,
        properties,
        outboxMetrics,
        resolveObservationRegistry(observationRegistry),
        resolveInstanceId(properties));
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxRecoveryService outboxRecoveryService(
      OutboxRepository outboxRepository, OutboxProperties properties, OutboxMetrics outboxMetrics) {
    return new DefaultOutboxRecoveryService(outboxRepository, properties, outboxMetrics);
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxCleanupService outboxCleanupService(
      OutboxRepository outboxRepository, OutboxProperties properties) {
    return new DefaultOutboxCleanupService(outboxRepository, properties);
  }

  /**
   * Kafka broker adapter, isolated so that spring-kafka stays an optional dependency. When
   * spring-kafka is absent from the classpath this class is skipped without its bean method ever
   * being introspected.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(KafkaTemplate.class)
  static class OutboxKafkaConfiguration {

    @Bean
    @ConditionalOnMissingBean(MessageBrokerPublisher.class)
    @ConditionalOnBean(KafkaTemplate.class)
    MessageBrokerPublisher kafkaOutboxPublisher(KafkaTemplate<String, String> kafkaTemplate) {
      return new KafkaOutboxPublisher(kafkaTemplate);
    }
  }

  private static ObservationRegistry resolveObservationRegistry(
      ObjectProvider<ObservationRegistry> observationRegistry) {
    return observationRegistry.getIfAvailable(() -> ObservationRegistry.NOOP);
  }

  /**
   * Fallback mapper for applications that have no {@code ObjectMapper} bean. Spring Boot only
   * contributes one when {@code Jackson2ObjectMapperBuilder} is on the classpath, which ships with
   * spring-web — so a headless relay or worker service has none.
   */
  private static ObjectMapper defaultObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  private static String resolveInstanceId(OutboxProperties properties) {
    if (properties.instanceId() != null && !properties.instanceId().isBlank()) {
      return properties.instanceId();
    }
    try {
      return InetAddress.getLocalHost().getHostName() + "-" + ProcessHandle.current().pid();
    } catch (Exception exception) {
      return "outbox-" + UUID.randomUUID();
    }
  }
}
