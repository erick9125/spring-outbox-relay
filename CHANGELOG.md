# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-SNAPSHOT] - unreleased

### Changed

- Claiming a batch is now a single statement. It was a SELECT, then one UPDATE per row, then a
  SELECT to read the rows back: `2 + batch-size` round trips every poll — 102 per second at the
  default settings — all while holding row locks. A CTE with `FOR UPDATE SKIP LOCKED` and
  `RETURNING` does the same work atomically in one.
- The recovery and cleanup jobs now work in bounded batches and drain in a loop, controlled by the
  new `spring.outbox.relay.maintenance-batch-size` (default 500). Both statements were unbounded:
  after a long outage, or against months of retained events, a single UPDATE or DELETE would hold
  locks across the entire backlog, bloat the table and compete with the relay for the same rows.
  `recoverAbandoned` and `deletePublishedBefore` take a row limit and report how many rows they
  touched.

### Added

- Configuration metadata for the properties that had none: `instance-id`, `recovery-interval`,
  `maintenance-batch-size`, `retry.*` and `cleanup.interval` now autocomplete in the IDE.
- Tests for the cleanup service, which had none at all.

### Fixed

- Headers attached to an `OutboxMessage` now reach the broker. They were serialized into the
  `headers` column and then never read: the Kafka adapter only ever wrote its own five metadata
  headers, so a documented feature — the `correlation-id` in the README's own example — was
  silently dropped on every publication. Names that clash with relay metadata are discarded with
  a warning rather than delivered as a second value for the same key.
- `publish()` now fails fast with `IllegalStateException` when no transaction is active. Called
  outside one, the outbox insert committed on its own and the atomicity the pattern exists for
  was silently gone. **Breaking** for callers that were (incorrectly) publishing outside a
  transaction.
- The Kafka adapter has tests. `src/test/.../broker/` was an empty directory, which is why the
  dropped headers went unnoticed.

- The terminal transitions (`markPublished`, `reschedule`, `markFailed`) are now fenced on the
  current claim owner: `AND status = 'PROCESSING' AND locked_by = ?`. A claim is a lease, so a
  stalled instance could previously overwrite the state of the instance that had taken the row
  over after `lock-timeout` — resurrecting a published event, rescheduling a delivered one, or
  marking a successful publication as failed. Lost claims are now reported through the new
  `outbox.events.lock.lost` metric and `RelayResult.lockLost()`, and the row is left untouched.
- `gradlew` is now committed with the executable bit set. Without it every Linux checkout —
  including CI — fails with `./gradlew: Permission denied` before any task can run.
- The CI push trigger now watches `master`, the actual default branch, instead of a `main`
  branch that never existed, so pushes to the default branch are checked at all.
- The CI matrix no longer claims to test Java 25. Gradle 8.14.3 cannot run on it at all (Java 24
  is its ceiling, Java 25 needs Gradle 9.1+), and the pinned toolchain meant the entry compiled
  and tested against Java 21 anyway.
- `markPublished` no longer fails when the broker adapter reports no message id. `jsonb_set` is
  strict, so a null id collapsed the `headers` column to NULL and violated its NOT NULL
  constraint; the relay then treated the already-published event as a retryable failure and
  republished it on every attempt until its budget ran out.

- Auto-configuration is now ordered after the `DataSource`, `JdbcTemplate`, Jackson, Kafka,
  metrics and observation auto-configurations. Without that ordering its `@ConditionalOnBean`
  checks ran too early, so no broker publisher, relay or scheduler was ever registered and
  events accumulated in the table without being published.
- The scheduler moved to its own top-level auto-configuration. As a nested `@Import`-ed class
  its `@ConditionalOnBean(OutboxRelay.class)` was evaluated before the relay bean definition
  existed and always resolved to false, so the relay, recovery and cleanup jobs never ran.
- Applications without an `ObjectMapper` bean now start. Spring Boot only contributes one when
  `Jackson2ObjectMapperBuilder` (from spring-web) is present, so headless relay and worker
  services failed with `NoSuchBeanDefinitionException`.
- The library no longer contributes an `ObservationRegistry` bean, which suppressed the host
  application's own registry along with its observation handlers and tracing.
- The shipped migration moved from `classpath:db/migration` to `classpath:db/outbox`. In the
  default location it collided with the consuming application's own `V1__…` migration and
  broke startup with `Found more than one migration with version 1`.
- Flyway is no longer a runtime dependency, so adding this library cannot activate
  `FlywayAutoConfiguration` against an application that does not use Flyway.
- `spring-kafka` is now optional (`compileOnly`) instead of an exported `api` dependency, and
  the Kafka adapter is guarded by `@ConditionalOnClass(KafkaTemplate.class)`.
- Micrometer is now an `api` dependency, as it appears in the public constructors of
  `OutboxMetrics`, `DefaultOutboxRelay` and `DefaultOutboxPublisher`.
- `./gradlew check` passes again. The Spotless target read the example project's build output,
  which Gradle rejects as an undeclared task dependency and failed the build.

### Added

- Transactional outbox persistence with Spring JDBC and PostgreSQL
- Concurrent batch claiming with `FOR UPDATE SKIP LOCKED`
- Kafka publication adapter with stable event headers
- Exponential backoff retry policy with jitter
- Abandoned lock recovery
- Failed event state after exhausted attempts
- Micrometer metrics and observation spans
- Published event cleanup
- Spring Boot auto-configuration
- Example `order-service`
- Testcontainers-backed integration tests
