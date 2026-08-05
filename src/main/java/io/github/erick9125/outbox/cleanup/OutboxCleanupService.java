package io.github.erick9125.outbox.cleanup;

public interface OutboxCleanupService {

  int cleanupPublishedEvents();
}
