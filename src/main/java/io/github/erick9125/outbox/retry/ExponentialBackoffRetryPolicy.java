package io.github.erick9125.outbox.retry;

import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.exception.RetryablePublicationException;
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
    if (initialDelay.isNegative() || initialDelay.isZero()) {
      throw new IllegalArgumentException("initialDelay must be positive");
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
      return RetryDecision.fail("permanent publication failure: " + rootMessage(failure));
    }

    int nextAttempt = event.attempts() + 1;
    if (nextAttempt >= event.maxAttempts()) {
      return RetryDecision.fail("max attempts exhausted (" + event.maxAttempts() + ")");
    }

    if (!isRetryable(failure)) {
      return RetryDecision.fail("non-retryable publication failure: " + rootMessage(failure));
    }

    Duration delay = computeDelay(nextAttempt);
    Instant nextAttemptAt = clock.instant().plus(delay);
    return RetryDecision.retryAt(nextAttemptAt, "retryable publication failure; delay=" + delay);
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

  private static boolean isRetryable(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof RetryablePublicationException) {
        return true;
      }
      if (current instanceof PermanentPublicationException) {
        return false;
      }
      current = current.getCause();
    }
    return true;
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
