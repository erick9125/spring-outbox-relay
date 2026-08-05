# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-SNAPSHOT] - unreleased

### Fixed

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
