# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-SNAPSHOT] - unreleased

### Fixed

- Permanent Kafka failures are recognised again. Spring for Apache Kafka wraps producer failures in
  a `KafkaProducerException`, so a rejected topic name arrives as
  `CompletionException → KafkaProducerException → InvalidTopicException`. The adapter only unwrapped
  the future's own layers and then classified the wrapper, which meant **every** real broker failure
  came out retryable: an event bound for an invalid topic was rescheduled with backoff over and over
  instead of failing once into an inspectable row. The whole cause chain is searched now.

### Added

- An end-to-end test that puts a real broker behind the relay, with Postgres and Kafka both on
  Testcontainers. It publishes through the outbox, relays, and consumes the record back to assert
  the payload, the record key, the five relay headers and the caller's own headers as a consumer
  sees them, plus the broker offset recorded on the row. The Kafka path had only ever been exercised
  against an in-memory `KafkaTemplate`; `org.testcontainers:kafka` was declared in the build and
  unused. This test is what found the classification bug above.

### Fixed

- The library can be published at all. `publishToMavenLocal` failed with *"Publication only
  contains dependencies and/or constraints without a version"*: every dependency is declared without
  one because the Spring Boot BOM supplies it through `io.spring.dependency-management`, which is a
  resolution-time mechanism that writes nothing into the published POM. The publication now writes
  resolved versions, so a consumer gets a POM it can actually resolve. Nothing had noticed because
  nothing had ever tried to publish.
- The published POM no longer carries `spring-boot-dependencies` as an imported BOM. The
  dependency-management plugin copies it in by default, which nudges a consuming build's Spring Boot
  versions towards ours; a library has no business doing that.
- `check` now generates the POM and fails if any dependency lacks a version, so this cannot regress
  unnoticed again.

### Fixed

- Invalid configuration now fails at startup instead of being silently replaced by a default.
  `batch-size: 0` quietly became 100, so a typo in a deployment behaved like a working
  configuration and the operator had no way to tell their value was ignored.
- `OutboxMessage` validates its string fields against the column widths, naming the field. An
  over-long value used to reach the insert and come back as a raw SQL error that took the caller's
  business transaction with it.
- `outbox.events.created` is incremented after the transaction commits. Counting at insert time
  kept the increment for rows that a business rollback removed.
- The relay repository uses the application's `PlatformTransactionManager` when there is one. It
  built a `DataSourceTransactionManager` of its own, which worked by binding to the same
  `DataSource` but ignored whatever the application had configured.
- `RetryDecision` rejects `retry = true` without a timestamp, and a blank reason. A custom policy
  returning either used to fail deep inside the repository instead of at the mistake.
- `ExponentialBackoffRetryPolicy` rejects a sub-millisecond `initial-delay`. Positive but rounding
  to zero milliseconds made the jitter bound zero, which `ThreadLocalRandom` rejects outright.
- `last_error` for an exhausted retry budget now also names the failure that exhausted it, and a
  truncated value ends with `… [truncated]` so a reader knows it was cut.
- Removed an unreachable branch in the retry policy. `isPermanent` already matched anything
  carrying a `PermanentPublicationException`, so the `non-retryable` path could never be taken and
  `isRetryable` could never return false. Unrecognised failures are retried on purpose — a retry
  costs a duplicate the consumer already deduplicates, a discard loses the event — and that is now
  stated in one place instead of implied by dead code.
- Dropped the `integrationTest` Gradle task, which duplicated `test`, and documented that
  `unitTest` is a Docker-free convenience deliberately not wired into `check`.

### Documented

- The payload constraint the library cannot check: PostgreSQL's `jsonb` rejects `U+0000` inside
  strings, so a payload carrying one fails the insert and the surrounding transaction with it.

### Fixed

- A poison event no longer loops forever. An event whose processing killed its worker was handed
  back to `PENDING` by recovery with nothing incremented, claimed again, and killed the next worker
  too. Migration `V2` adds a `recoveries` column, bounded by the new
  `spring.outbox.relay.max-recoveries` (default 3); past the budget the event becomes `FAILED` with
  `last_error` set and is counted in the new `outbox.events.recovery.exhausted` metric.
  The counter is separate from `attempts` deliberately: deploys interrupt in-flight publications,
  so charging recoveries to the publication retry budget would send healthy events to `FAILED`
  after a few releases without a single failed publication.

### Changed

- Migration `V2` replaces the indexes with partial ones matching what the jobs query.
  `idx_outbox_event_polling` led with `status`, which made `available_at` a range predicate and left
  the polling query sorting on every run, and it indexed every row including the `PUBLISHED` ones
  that dominate the table. The cleanup job's `status = 'PUBLISHED' AND published_at < ?` had no
  usable index at all and scanned every published row.

- The jobs run on a thread pool the library owns, one thread each, instead of on `@Scheduled`
  methods. The library no longer turns on `@EnableScheduling` for the whole host application, and
  a cleanup run deleting a large backlog can no longer block the relay: all four jobs previously
  shared Spring Boot's default scheduler, whose pool size is one. The pool is deliberately not
  exposed as a bean, since a second `TaskScheduler` changes which one the application's own
  `@Scheduled` methods resolve to.
- Shutdown is graceful. `OutboxScheduler` is a `SmartLifecycle` that stops scheduling and then
  waits up to the new `spring.outbox.relay.shutdown-timeout` (default 30s) for the run in
  progress. Every deploy used to kill the relay mid-publication, leaving up to `batch-size` rows
  in `PROCESSING` until `lock-timeout` elapsed — minutes of delay on every release.
- The backlog gauges read cached values, refreshed on the new
  `spring.outbox.relay.backlog-metrics-interval` (default 10s). They ran `COUNT(*)` and an
  `ORDER BY` against the outbox table on every scrape, on the scrape thread and holding a pooled
  connection, so a slow database could hang `/actuator/prometheus` and each extra collector
  multiplied the cost. They are also registered so the registry keeps a strong reference; the
  previous form held the state weakly and would report `NaN` once it was collected.
- A job that throws no longer stops being scheduled, and the failure is logged with the name of
  the job that failed.

### Documented

- Ordering. Events are **not** delivered in order, not even per aggregate: `SKIP LOCKED` spreads
  rows across instances, a batch is published concurrently, and a rescheduled event arrives after
  the ones behind it. `partitionKey` chooses the Kafka partition, which bounds where ordering
  could hold, but the relay never serialises the events sharing a key. The README implied
  otherwise.

- The relay hands the whole claimed batch to the broker before awaiting any of it, and settles it
  against a single deadline: the new `spring.outbox.relay.publish-timeout` (default 30s). It used
  to await each acknowledgement before sending the next, so a batch cost the sum of its latencies
  — with `batch-size: 100` and a stuck broker, 100 timeouts back to back, far longer than the 5
  minute `lock-timeout`. The recovery job would then reclaim rows the relay was still publishing.
  Keep `publish-timeout` below `lock-timeout`.
- `MessageBrokerPublisher.publish` returns `CompletableFuture<PublicationResult>` so adapters do
  not block the batch. **Breaking** for custom adapters; a synchronous one only needs
  `CompletableFuture.completedFuture(...)`.
- `outbox.publication.duration` is now tagged `result=success|failure` and measured on the
  broker's acknowledgement rather than around a blocking call, so a 30 second timeout no longer
  sits in the same distribution as successful publications.
- `last_error` names the failure that caused a reschedule. It recorded only
  `retryable publication failure; delay=PT5S`, leaving an operator with no idea what went wrong;
  the wrapper's message and the root cause are both kept when they differ.

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
