package io.github.erick9125.outbox.recovery;

public interface OutboxRecoveryService {

  int recoverAbandonedEvents();
}
