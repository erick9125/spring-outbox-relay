package io.github.erick9125.outbox.retry;

import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class ExponentialBackoffRetryPolicy implements RetryPolicy {

  private final Duration initialDelay;
  private final Duration maximumDelay;
  private final double multiplier;
  private final double jitter;
  private final Clock clock;

  public ExponentialBackoffRetryPolicy(
      Duration initialDelay, Duration maximumDelay, double multiplier, double jitter) {
    this(initialDelay, maximumDelay, multiplier, jitter, Clock.systemUTC());
  }

  public ExponentialBackoffRetryPolicy(
      Duration initialDelay, Duration maximumDelay, double multiplier, double jitter, Clock clock) {
    // Milliseconds are the unit the delay is computed in, so a positive but sub-millisecond delay
    // rounds to zero and makes the jitter bound zero, which ThreadLocalRandom rejects outright.
    if (initialDelay.isNegative() || initialDelay.isZero() || initialDelay.toMillis() < 1) {
      throw new IllegalArgumentException("initialDelay must be at least 1ms, was " + initialDelay);
    }
    if (maximumDelay.compareTo(initialDelay) < 0) {
      throw new IllegalArgumentException("maximumDelay must be >= initialDelay");
    }
    if (multiplier < 1.0d) {
      throw new IllegalArgumentException("multiplier must be >= 1.0");
    }
    if (jitter < 0.0d || jitter > 1.0d) {
      throw new IllegalArgumentException("jitter must be between 0.0 and 1.0");
    }
    this.initialDelay = initialDelay;
    this.maximumDelay = maximumDelay;
    this.multiplier = multiplier;
    this.jitter = jitter;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public RetryDecision evaluate(OutboxEvent event, Throwable failure) {
    if (isPermanent(failure)) {
      return RetryDecision.fail("permanent publication failure: " + describe(failure));
    }

    // Everything that is not explicitly permanent is retried, including exceptions this library has
    // never seen. Discarding an event because of an unexpected error would be the worse failure: a
    // retry costs a duplicate the consumer already deduplicates, a discard loses the event.
    int nextAttempt = event.attempts() + 1;
    if (nextAttempt >= event.maxAttempts()) {
      return RetryDecision.fail(
          "max attempts exhausted ("
              + event.maxAttempts()
              + "); last failure: "
              + describe(failure));
    }

    Duration delay = computeDelay(nextAttempt);
    Instant nextAttemptAt = clock.instant().plus(delay);
    // The reason lands in last_error, so it has to name what actually failed. Recording only the
    // delay left an operator looking at a PENDING row with no idea why it was rescheduled.
    return RetryDecision.retryAt(
        nextAttemptAt, "retryable publication failure; delay=" + delay + "; " + describe(failure));
  }

  Duration computeDelay(int attemptNumber) {
    double factor = Math.pow(multiplier, Math.max(0, attemptNumber - 1));
    long rawMillis = Math.round(initialDelay.toMillis() * factor);
    long cappedMillis = Math.min(rawMillis, maximumDelay.toMillis());
    if (jitter == 0.0d) {
      return Duration.ofMillis(cappedMillis);
    }
    double bound = cappedMillis * jitter;
    long jitterOffset = Math.round(ThreadLocalRandom.current().nextDouble(-bound, bound));
    long withJitter = Math.max(1L, cappedMillis + jitterOffset);
    return Duration.ofMillis(Math.min(withJitter, maximumDelay.toMillis()));
  }

  private static boolean isPermanent(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof PermanentPublicationException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Describes a failure for {@code last_error}, which is the only record an operator has of why a
   * row is where it is.
   *
   * <p>Reporting only the root cause loses the wrapper's message, and the wrapper is often the
   * informative one — "broker did not acknowledge within 30s" wrapping a bare {@code
   * TimeoutException}. Reporting only the wrapper loses the opposite case, where the adapter's
   * generic message wraps the broker's specific one. So both are kept when they differ.
   */
  private static String describe(Throwable failure) {
    String root = rootMessage(failure);
    String message = failure.getMessage();
    if (message == null || message.isBlank() || message.equals(root)) {
      return root;
    }
    return message + " (" + root + ")";
  }

  private static String rootMessage(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    String message = current.getMessage();
    return message == null || message.isBlank() ? current.getClass().getSimpleName() : message;
  }
}
