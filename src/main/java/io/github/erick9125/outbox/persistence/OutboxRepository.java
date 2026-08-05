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

  void markPublished(UUID id, Instant publishedAt, String brokerMessageId);

  void reschedule(UUID id, int attempts, Instant availableAt, String lastError);

  void markFailed(UUID id, int attempts, String lastError);

  int recoverAbandoned(Instant lockedBefore);

  int deletePublishedBefore(Instant publishedBefore);

  long countPending();

  Optional<Instant> findOldestPendingAvailableAt();

  Optional<OutboxEvent> findById(UUID id);
}
