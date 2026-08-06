package io.github.erick9125.outbox.configuration;

import io.github.erick9125.outbox.cleanup.OutboxCleanupService;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.recovery.OutboxRecoveryService;
import io.github.erick9125.outbox.relay.OutboxRelay;
import io.github.erick9125.outbox.scheduling.OutboxScheduler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Drives the relay, recovery and cleanup jobs on a schedule.
 *
 * <p>This is a separate auto-configuration rather than a nested class of {@link
 * OutboxAutoConfiguration} on purpose. Nested and {@code @Import}-ed configuration classes are
 * parsed before the importing class registers its own {@code @Bean} definitions, so a
 * {@code @ConditionalOnBean(OutboxRelay.class)} declared there would always evaluate to false and
 * the scheduler would silently never be registered. As a top-level auto-configuration ordered after
 * {@link OutboxAutoConfiguration}, the relay bean definition is guaranteed to be visible.
 */
@AutoConfiguration
@AutoConfigureAfter(OutboxAutoConfiguration.class)
@ConditionalOnBean(OutboxRelay.class)
@ConditionalOnProperty(
    prefix = "spring.outbox.relay",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class OutboxSchedulingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  OutboxScheduler outboxScheduler(
      OutboxRelay outboxRelay,
      OutboxRecoveryService outboxRecoveryService,
      OutboxCleanupService outboxCleanupService,
      OutboxMetrics outboxMetrics,
      OutboxProperties properties) {
    return new OutboxScheduler(
        outboxRelay, outboxRecoveryService, outboxCleanupService, outboxMetrics, properties);
  }
}
