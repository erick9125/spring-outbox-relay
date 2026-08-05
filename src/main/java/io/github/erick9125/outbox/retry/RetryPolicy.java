package io.github.erick9125.outbox.retry;

import io.github.erick9125.outbox.domain.OutboxEvent;

public interface RetryPolicy {

  RetryDecision evaluate(OutboxEvent event, Throwable failure);
}
