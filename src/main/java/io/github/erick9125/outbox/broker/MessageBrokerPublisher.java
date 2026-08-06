package io.github.erick9125.outbox.broker;

import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import java.util.concurrent.CompletableFuture;

/** Adapter that hands an outbox event to a message broker. */
public interface MessageBrokerPublisher {

  /**
   * Hands the event to the broker and returns a future that completes when the broker acknowledges
   * it.
   *
   * <p>Implementations must not block waiting for the acknowledgement. The relay hands off a whole
   * batch before awaiting any of it, so a blocking adapter turns the batch back into a serial
   * chain: at the default settings that is up to 100 acknowledgements one after another, which can
   * outlast {@code lock-timeout} and get the batch reclaimed while it is still being published.
   * Return the broker client's own future instead.
   *
   * <p>Report failures by completing the future exceptionally, or by throwing. Use {@link
   * io.github.erick9125.outbox.exception.PermanentPublicationException} for failures that cannot
   * succeed on a retry and {@link
   * io.github.erick9125.outbox.exception.RetryablePublicationException} for transient ones;
   * anything else is treated as retryable.
   */
  CompletableFuture<PublicationResult> publish(OutboxEvent event);
}
