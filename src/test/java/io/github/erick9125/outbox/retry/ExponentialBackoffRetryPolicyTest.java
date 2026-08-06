package io.github.erick9125.outbox.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  @Test
  void retriesAnUnrecognisedFailure() {
    // Anything not explicitly permanent is retried, including exceptions this library has never
    // seen. A retry costs a duplicate the consumer already deduplicates; a discard loses the event.
    RetryDecision decision = policy.evaluate(event(0, 5), new IllegalStateException("who knows"));

    assertThat(decision.retry()).isTrue();
    assertThat(decision.reason()).contains("who knows");
  }

  @Test
  void treatsAPermanentCauseWrappedInSomethingElseAsPermanent() {
    RetryDecision decision =
        policy.evaluate(
            event(0, 5),
            new IllegalStateException("wrapper", new PermanentPublicationException("bad topic")));

    assertThat(decision.retry()).isFalse();
    assertThat(decision.reason()).contains("permanent").contains("bad topic");
  }

  @Test
  void recordsTheFailureThatExhaustedTheBudget() {
    RetryDecision decision =
        policy.evaluate(event(4, 5), new RetryablePublicationException("broker unreachable"));

    assertThat(decision.reason()).contains("max attempts").contains("broker unreachable");
  }

  @Test
  void keepsBothTheWrapperMessageAndTheRootCause() {
    // Reporting only the root cause loses "did not acknowledge in 30s" wrapping a bare
    // TimeoutException; reporting only the wrapper loses the broker's own specific message.
    RetryDecision decision =
        policy.evaluate(
            event(0, 5),
            new RetryablePublicationException(
                "broker did not acknowledge", new IllegalStateException("connection reset")));

    assertThat(decision.reason())
        .contains("broker did not acknowledge")
        .contains("connection reset");
  }

  @Test
  void rejectsASubMillisecondInitialDelay() {
    // Positive but rounding to zero milliseconds, which makes the jitter bound zero and
    // ThreadLocalRandom throw.
    assertThatThrownBy(
            () ->
                new ExponentialBackoffRetryPolicy(
                    Duration.ofNanos(500), Duration.ofMinutes(5), 2.0d, 0.2d))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 1ms");
  }

  @Test
  void rejectsARetryDecisionWithoutATimestamp() {
    // A custom policy returning this would otherwise fail deep inside the repository.
    assertThatThrownBy(() -> new RetryDecision(true, null, "later"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nextAttemptAt is required");

    assertThatThrownBy(() -> RetryDecision.fail("  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("last_error");
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
        0,
        now,
        now,
        now,
        now,
        "worker",
        null,
        null);
  }
}
