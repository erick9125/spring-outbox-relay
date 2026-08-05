# Comparison with Spring Modulith

Spring Modulith includes a persistent application-event publication registry with
incomplete publication recovery.

Spring Outbox Relay is not positioned as "better than Spring Modulith" without
benchmarks or broader adoption evidence.

## Choose Spring Outbox Relay when you need

- An explicit outbox table you can query and operate on directly
- Direct ownership of schema and indexes
- Concurrent polling with `SKIP LOCKED`
- Pluggable broker adapters
- Operational states (`PENDING`, `PROCESSING`, `PUBLISHED`, `FAILED`)
- Configurable retry lifecycle and backlog metrics
- Usage without adopting Spring Modulith module boundaries

## Choose Spring Modulith when you already

- Model your system with Spring Modulith application modules
- Want integrated application-event publication tracking
- Prefer framework-owned event publication plumbing over an explicit outbox API

## Positioning

A focused alternative for teams that need direct control over the outbox table,
polling strategy, retry lifecycle, and broker publication.
