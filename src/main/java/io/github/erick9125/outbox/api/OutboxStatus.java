package io.github.erick9125.outbox.api;

public enum OutboxStatus {
  PENDING,
  PROCESSING,
  PUBLISHED,
  FAILED
}
