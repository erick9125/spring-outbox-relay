package io.github.erick9125.outbox.retry;

import java.time.Instant;

public record RetryDecision(boolean retry, Instant nextAttemptAt, String reason) {

  public static RetryDecision retryAt(Instant nextAttemptAt, String reason) {
    return new RetryDecision(true, nextAttemptAt, reason);
  }

  public static RetryDecision fail(String reason) {
    return new RetryDecision(false, null, reason);
  }
}
