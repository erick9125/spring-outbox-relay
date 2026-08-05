package io.github.erick9125.outbox.domain;

/**
 * Outcome of a single relay pass.
 *
 * @param claimed events claimed from the outbox
 * @param published events accepted by the broker and marked {@code PUBLISHED}
 * @param rescheduled events returned to {@code PENDING} for a later attempt
 * @param failed events moved to {@code FAILED}
 * @param lockLost events whose claim had already been reclaimed by another instance before this one
 *     could record an outcome, so the row was left untouched
 */
public record RelayResult(int claimed, int published, int rescheduled, int failed, int lockLost) {}
