package io.github.erick9125.outbox.configuration;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "spring.outbox.relay")
public record OutboxProperties(
    @DefaultValue("true") boolean enabled,
    @DefaultValue("100") int batchSize,
    @DefaultValue("1s") Duration pollInterval,
    @DefaultValue("5m") Duration lockTimeout,
    @DefaultValue("5") int defaultMaxAttempts,
    String instanceId,
    @DefaultValue("1m") Duration recoveryInterval,
    @DefaultValue("500") int maintenanceBatchSize,
    @DefaultValue("3") int maxRecoveries,
    @DefaultValue("30s") Duration publishTimeout,
    @DefaultValue("10s") Duration backlogMetricsInterval,
    @DefaultValue("30s") Duration shutdownTimeout,
    @DefaultValue Retry retry,
    @DefaultValue Cleanup cleanup) {

  public OutboxProperties {
    if (batchSize < 1) {
      batchSize = 100;
    }
    if (defaultMaxAttempts < 1) {
      defaultMaxAttempts = 5;
    }
    if (maintenanceBatchSize < 1) {
      maintenanceBatchSize = 500;
    }
    // Deploys interrupt in-flight publications, so a handful of recoveries is normal operation and
    // the budget only has to be tight enough to catch an event that keeps killing its worker.
    if (maxRecoveries < 1) {
      maxRecoveries = 3;
    }
    // A batch is awaited against this as a single deadline, so it also bounds how long a poll can
    // hold its claims. Keeping it below lock-timeout is what stops the recovery job from reclaiming
    // rows that are still in flight.
    if (publishTimeout == null || publishTimeout.isNegative() || publishTimeout.isZero()) {
      publishTimeout = Duration.ofSeconds(30);
    }
    if (backlogMetricsInterval == null
        || backlogMetricsInterval.isNegative()
        || backlogMetricsInterval.isZero()) {
      backlogMetricsInterval = Duration.ofSeconds(10);
    }
    if (shutdownTimeout == null || shutdownTimeout.isNegative()) {
      shutdownTimeout = Duration.ofSeconds(30);
    }
    if (retry == null) {
      retry = Retry.defaults();
    }
    if (cleanup == null) {
      cleanup = Cleanup.defaults();
    }
  }

  public static OutboxProperties defaults() {
    return new OutboxProperties(
        true,
        100,
        Duration.ofSeconds(1),
        Duration.ofMinutes(5),
        5,
        null,
        Duration.ofMinutes(1),
        500,
        3,
        Duration.ofSeconds(30),
        Duration.ofSeconds(10),
        Duration.ofSeconds(30),
        Retry.defaults(),
        Cleanup.defaults());
  }

  public record Retry(
      @DefaultValue("5s") Duration initialDelay,
      @DefaultValue("5m") Duration maximumDelay,
      @DefaultValue("2.0") double multiplier,
      @DefaultValue("0.2") double jitter) {

    public static Retry defaults() {
      return new Retry(Duration.ofSeconds(5), Duration.ofMinutes(5), 2.0d, 0.2d);
    }
  }

  public record Cleanup(
      @DefaultValue("true") boolean enabled,
      @DefaultValue("7d") Duration retention,
      @DefaultValue("1h") Duration interval) {

    public static Cleanup defaults() {
      return new Cleanup(true, Duration.ofDays(7), Duration.ofHours(1));
    }
  }
}
