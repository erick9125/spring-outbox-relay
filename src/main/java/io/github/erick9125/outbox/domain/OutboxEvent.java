package io.github.erick9125.outbox.domain;

import io.github.erick9125.outbox.api.OutboxStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * A row of the outbox table.
 *
 * @param attempts publication attempts spent, bounded by {@code maxAttempts}
 * @param recoveries times this event's claim was found abandoned and returned to {@code PENDING},
 *     bounded by {@code max-recoveries}. Counted separately from {@code attempts} because the two
 *     failure modes are unrelated: a deploy interrupting an in-flight publication should not spend
 *     the retry budget of an event that never actually failed to publish.
 */
public record OutboxEvent(
    UUID id,
    String aggregateType,
    String aggregateId,
    String eventType,
    int eventVersion,
    String destination,
    String partitionKey,
    String payload,
    String headers,
    OutboxStatus status,
    int attempts,
    int maxAttempts,
    int recoveries,
    Instant occurredAt,
    Instant createdAt,
    Instant availableAt,
    Instant lockedAt,
    String lockedBy,
    Instant publishedAt,
    String lastError) {}
