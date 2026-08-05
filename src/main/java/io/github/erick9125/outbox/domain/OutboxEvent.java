package io.github.erick9125.outbox.domain;

import io.github.erick9125.outbox.api.OutboxStatus;
import java.time.Instant;
import java.util.UUID;

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
    Instant occurredAt,
    Instant createdAt,
    Instant availableAt,
    Instant lockedAt,
    String lockedBy,
    Instant publishedAt,
    String lastError) {}
