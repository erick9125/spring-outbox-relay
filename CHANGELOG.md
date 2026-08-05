# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-SNAPSHOT] - unreleased

### Fixed

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
