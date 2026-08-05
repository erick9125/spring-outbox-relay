package io.github.erick9125.outbox.domain;

public record RelayResult(int claimed, int published, int rescheduled, int failed) {}
