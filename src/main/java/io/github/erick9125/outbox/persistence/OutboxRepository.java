package io.github.erick9125.outbox.persistence;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.domain.OutboxEvent;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxRepository {

  UUID insert(OutboxMessage message, String payloadJson, String headersJson, Instant occurredAt);

  List<OutboxEvent> claimBatch(int batchSize, String lockedBy);

  /**
   * Records a successful publication, but only while {@code lockedBy} still owns the row.
   *
   * <p>A claim is not a lease the owner can rely on indefinitely: once {@code lock-timeout} elapses
   * the recovery job returns the row to {@code PENDING} and another instance may claim and publish
   * it. An unconditional update would then let the original owner overwrite the new owner's state —
   * silently resurrecting a published event, or marking a row failed that was in fact delivered.
   * All three terminal transitions are therefore fenced on the current owner.
   *
   * @return {@code true} if the row was still owned by {@code lockedBy} and was updated, {@code
   *     false} if the claim had already been lost
   */
  boolean markPublished(UUID id, String lockedBy, Instant publishedAt, String brokerMessageId);

  /**
   * Returns the event to {@code PENDING} for a later attempt, only while {@code lockedBy} still
   * owns the row.
   *
   * @return {@code true} if the row was still owned by {@code lockedBy} and was updated
   */
  boolean reschedule(UUID id, String lockedBy, int attempts, Instant availableAt, String lastError);

  /**
   * Moves the event to {@code FAILED}, only while {@code lockedBy} still owns the row.
   *
   * @return {@code true} if the row was still owned by {@code lockedBy} and was updated
   */
  boolean markFailed(UUID id, String lockedBy, int attempts, String lastError);

  int recoverAbandoned(Instant lockedBefore);

  int deletePublishedBefore(Instant publishedBefore);

  long countPending();

  Optional<Instant> findOldestPendingAvailableAt();

  Optional<OutboxEvent> findById(UUID id);
}
