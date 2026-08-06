package io.github.erick9125.outbox.retry;

import java.time.Instant;

/**
 * What a {@link RetryPolicy} decided about a failed publication.
 *
 * @param retry whether the event should be tried again
 * @param nextAttemptAt when to try again; required when {@code retry} is true, ignored otherwise
 * @param reason recorded in the event's {@code last_error} for an operator to read
 */
public record RetryDecision(boolean retry, Instant nextAttemptAt, String reason) {

  public RetryDecision {
    // A custom policy returning retry=true with no timestamp would blow up later inside the
    // repository, far from the mistake. Reject it here, where the message can name the cause.
    if (retry && nextAttemptAt == null) {
      throw new IllegalArgumentException("nextAttemptAt is required when retry is true");
    }
    if (reason == null || reason.isBlank()) {
      throw new IllegalArgumentException("reason must not be blank; it is stored in last_error");
    }
  }

  public static RetryDecision retryAt(Instant nextAttemptAt, String reason) {
    return new RetryDecision(true, nextAttemptAt, reason);
  }

  public static RetryDecision fail(String reason) {
    return new RetryDecision(false, null, reason);
  }
}
