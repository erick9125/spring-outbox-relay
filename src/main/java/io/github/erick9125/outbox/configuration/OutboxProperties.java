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

  /**
   * Rejects invalid configuration instead of quietly substituting a default.
   *
   * <p>Silently correcting {@code batch-size: 0} back to 100 meant a typo in a deployment behaved
   * like a working configuration, and the operator had no way to tell that the value they set was
   * being ignored. {@code @DefaultValue} already covers properties that are simply absent, so
   * anything reaching these checks was set explicitly and set wrong.
   */
  public OutboxProperties {
    requirePositive(batchSize, "batch-size");
    requirePositive(defaultMaxAttempts, "default-max-attempts");
    requirePositive(maintenanceBatchSize, "maintenance-batch-size");
    // Deploys interrupt in-flight publications, so a handful of recoveries is normal operation and
    // the budget only has to be tight enough to catch an event that keeps killing its worker.
    requirePositive(maxRecoveries, "max-recoveries");

    requirePositive(pollInterval, "poll-interval");
    requirePositive(lockTimeout, "lock-timeout");
    requirePositive(recoveryInterval, "recovery-interval");
    // A batch is awaited against this as a single deadline, so it also bounds how long a poll can
    // hold its claims. Keeping it below lock-timeout is what stops the recovery job from reclaiming
    // rows that are still in flight.
    requirePositive(publishTimeout, "publish-timeout");
    requirePositive(backlogMetricsInterval, "backlog-metrics-interval");
    if (shutdownTimeout == null || shutdownTimeout.isNegative()) {
      throw new IllegalArgumentException(
          "spring.outbox.relay.shutdown-timeout must not be negative, was " + shutdownTimeout);
    }

    if (retry == null) {
      retry = Retry.defaults();
    }
    if (cleanup == null) {
      cleanup = Cleanup.defaults();
    }
  }

  private static void requirePositive(int value, String property) {
    if (value < 1) {
      throw new IllegalArgumentException(
          "spring.outbox.relay." + property + " must be at least 1, was " + value);
    }
  }

  private static void requirePositive(Duration value, String property) {
    if (value == null || value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException(
          "spring.outbox.relay." + property + " must be a positive duration, was " + value);
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
