package io.github.erick9125.outbox.retry;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.exception.RetryablePublicationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ExponentialBackoffRetryPolicyTest {

  private final Clock clock = Clock.fixed(Instant.parse("2026-08-03T12:00:00Z"), ZoneOffset.UTC);
  private final ExponentialBackoffRetryPolicy policy =
      new ExponentialBackoffRetryPolicy(
          Duration.ofSeconds(5), Duration.ofMinutes(5), 2.0d, 0.0d, clock);

  @Test
  void usesExponentialBackoff() {
    assertThat(policy.computeDelay(1)).isEqualTo(Duration.ofSeconds(5));
    assertThat(policy.computeDelay(2)).isEqualTo(Duration.ofSeconds(10));
    assertThat(policy.computeDelay(3)).isEqualTo(Duration.ofSeconds(20));
    assertThat(policy.computeDelay(4)).isEqualTo(Duration.ofSeconds(40));
  }

  @Test
  void retriesRetryableFailures() {
    OutboxEvent event = event(0, 5);
    RetryDecision decision = policy.evaluate(event, new RetryablePublicationException("timeout"));
    assertThat(decision.retry()).isTrue();
    assertThat(decision.nextAttemptAt()).isEqualTo(Instant.parse("2026-08-03T12:00:05Z"));
  }

  @Test
  void failsPermanentErrors() {
    RetryDecision decision =
        policy.evaluate(event(0, 5), new PermanentPublicationException("bad topic"));
    assertThat(decision.retry()).isFalse();
  }

  @Test
  void failsWhenAttemptsExhausted() {
    RetryDecision decision =
        policy.evaluate(event(4, 5), new RetryablePublicationException("timeout"));
    assertThat(decision.retry()).isFalse();
    assertThat(decision.reason()).contains("max attempts");
  }

  private static OutboxEvent event(int attempts, int maxAttempts) {
    Instant now = Instant.parse("2026-08-03T12:00:00Z");
    return new OutboxEvent(
        UUID.randomUUID(),
        "ORDER",
        "1",
        "order.created",
        1,
        "orders.events",
        "1",
        "{}",
        "{}",
        OutboxStatus.PROCESSING,
        attempts,
        maxAttempts,
        now,
        now,
        now,
        now,
        "worker",
        null,
        null);
  }
}
