package io.github.erick9125.outbox.api;

import java.util.UUID;

public interface OutboxPublisher {

  UUID publish(OutboxMessage message);
}
