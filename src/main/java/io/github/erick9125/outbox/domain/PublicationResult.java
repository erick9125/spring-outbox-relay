package io.github.erick9125.outbox.domain;

import java.time.Instant;

/**
 * Acknowledgement returned by a {@link io.github.erick9125.outbox.broker.MessageBrokerPublisher}
 * once the broker has accepted a message.
 *
 * @param brokerMessageId broker-assigned identifier, stored on the outbox row for traceability. May
 *     be {@code null}: not every broker returns one, and adapters should not invent a value.
 * @param publishedAt when the broker accepted the message
 */
public record PublicationResult(String brokerMessageId, Instant publishedAt) {}
