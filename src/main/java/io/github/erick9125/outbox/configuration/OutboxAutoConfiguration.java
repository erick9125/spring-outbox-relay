package io.github.erick9125.outbox.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.github.erick9125.outbox.scheduling.OutboxScheduler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.net.InetAddress;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@EnableConfigurationProperties(OutboxProperties.class)
@ConditionalOnClass(JdbcTemplate.class)
@ConditionalOnProperty(
    prefix = "spring.outbox.relay",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@Import(OutboxAutoConfiguration.OutboxSchedulingConfiguration.class)
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
  ObservationRegistry outboxObservationRegistry() {
    return ObservationRegistry.create();
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxPublisher outboxPublisher(
      OutboxRepository outboxRepository,
      ObjectMapper objectMapper,
      OutboxMetrics outboxMetrics,
      ObservationRegistry observationRegistry) {
    return new DefaultOutboxPublisher(
        outboxRepository, objectMapper, outboxMetrics, observationRegistry);
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
  @ConditionalOnBean(KafkaTemplate.class)
  MessageBrokerPublisher kafkaOutboxPublisher(KafkaTemplate<String, String> kafkaTemplate) {
    return new KafkaOutboxPublisher(kafkaTemplate);
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
      ObservationRegistry observationRegistry) {
    return new DefaultOutboxRelay(
        outboxRepository,
        messageBrokerPublisher,
        retryPolicy,
        properties,
        outboxMetrics,
        observationRegistry,
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

  @EnableScheduling
  @ConditionalOnBean(OutboxRelay.class)
  @ConditionalOnProperty(
      prefix = "spring.outbox.relay",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  static class OutboxSchedulingConfiguration {

    @Bean
    @ConditionalOnMissingBean
    OutboxScheduler outboxScheduler(
        OutboxRelay outboxRelay,
        OutboxRecoveryService outboxRecoveryService,
        OutboxCleanupService outboxCleanupService) {
      return new OutboxScheduler(outboxRelay, outboxRecoveryService, outboxCleanupService);
    }
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
