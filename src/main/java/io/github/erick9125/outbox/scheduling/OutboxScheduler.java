package io.github.erick9125.outbox.scheduling;

import io.github.erick9125.outbox.cleanup.OutboxCleanupService;
import io.github.erick9125.outbox.recovery.OutboxRecoveryService;
import io.github.erick9125.outbox.relay.OutboxRelay;
import org.springframework.scheduling.annotation.Scheduled;

public final class OutboxScheduler {

  private final OutboxRelay outboxRelay;
  private final OutboxRecoveryService recoveryService;
  private final OutboxCleanupService cleanupService;

  public OutboxScheduler(
      OutboxRelay outboxRelay,
      OutboxRecoveryService recoveryService,
      OutboxCleanupService cleanupService) {
    this.outboxRelay = outboxRelay;
    this.recoveryService = recoveryService;
    this.cleanupService = cleanupService;
  }

  @Scheduled(fixedDelayString = "${spring.outbox.relay.poll-interval:1s}")
  public void relay() {
    outboxRelay.relayBatch();
  }

  @Scheduled(fixedDelayString = "${spring.outbox.relay.recovery-interval:1m}")
  public void recover() {
    recoveryService.recoverAbandonedEvents();
  }

  @Scheduled(fixedDelayString = "${spring.outbox.relay.cleanup.interval:1h}")
  public void cleanup() {
    cleanupService.cleanupPublishedEvents();
  }
}
