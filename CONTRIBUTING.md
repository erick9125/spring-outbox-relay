# Contributing

Thanks for your interest in Spring Outbox Relay.

## Development

Requirements:

- JDK 21+
- Docker (for Testcontainers)

Useful commands:

```bash
./gradlew test
./gradlew check
./gradlew spotlessApply
```

## Guidelines

- Keep 0.1.0 focused on PostgreSQL → Kafka outbox relay
- Do not promise exactly-once delivery
- Prefer JDBC and explicit SQL for outbox operations
- Avoid high-cardinality metric tags (`event_id`, `aggregate_id`, full error messages)
- Add or update tests for concurrency, rollback, retry, and recovery behavior

## Pull requests

1. Keep changes focused
2. Include tests for behavioral changes
3. Update docs when guarantees or configuration change
4. Ensure `./gradlew check` passes
