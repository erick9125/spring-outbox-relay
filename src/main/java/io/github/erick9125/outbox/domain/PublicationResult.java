package io.github.erick9125.outbox.domain;

import java.time.Instant;

public record PublicationResult(String brokerMessageId, Instant publishedAt) {}
