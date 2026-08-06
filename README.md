# Spring Outbox Relay

**Reliably publish database-backed events from Spring Boot applications.**

Spring Outbox Relay stores domain events in the same PostgreSQL transaction as your
business changes, then publishes them to Apache Kafka through a concurrent,
recoverable, and observable relay.

[English](README.md) · [Español](README.es.md)

---

## Why this library

Persisting a change in PostgreSQL and publishing a message to Kafka are not one
distributed transaction. If the database commit succeeds and Kafka fails, consumers
never see the event and the system drifts.

The transactional outbox pattern solves that inconsistency by writing the event to an
`outbox_event` table in the same transaction as the business data. The hard part is
everything that happens after the commit:

- claiming pending rows without double-processing
- publishing under failure and restart
- retrying recoverable errors
- recovering abandoned locks
- exposing backlog health without noisy metrics

Spring Outbox Relay focuses on that operational relay — not on teaching the pattern,
and not on becoming a full event platform.

| Concern | How this library helps |
| --- | --- |
| Dual-write inconsistency | Business row + outbox event share one PostgreSQL transaction |
| Concurrent workers | `FOR UPDATE SKIP LOCKED` claims disjoint batches |
| Broker outages | Retryable failures are rescheduled with backoff and jitter |
| Crashed instances | Abandoned `PROCESSING` locks are recovered automatically |
| Poison messages | Exhausted or permanent failures become inspectable `FAILED` rows |
| Operations | Micrometer metrics and observation spans for backlog and publish latency |
| Ownership | Explicit schema, states, and JDBC control without adopting Spring Modulith |

> Save the event with the business change. Relay it to Kafka after commit. Survive failure.

---

## What it does

1. Accepts an `OutboxMessage` inside a Spring `@Transactional` boundary
2. Serializes the payload and inserts an `outbox_event` row
3. Commits together with your business writes, or rolls back with them
4. Polls pending events in batches
5. Claims a batch with a short database transaction
6. Publishes each event to Kafka
7. Marks the row `PUBLISHED`, reschedules it, or marks it `FAILED`

```text
Use case
   │
   ▼
PostgreSQL transaction
   ├── business entity
   └── outbox event
           │
           ▼
         commit
           │
           ▼
Relay claims PENDING batch (SKIP LOCKED)
           │
           ▼
Publish to Kafka
           │
           ├─► PUBLISHED
           ├─► PENDING   (retry later)
           └─► FAILED    (exhausted / permanent)
```

---

## Delivery guarantees

Spring Outbox Relay provides **at-least-once** publication.

An event may be delivered more than once when a failure occurs after Kafka accepts the
message and before the outbox row is marked `PUBLISHED`. Consumers must deduplicate
using the stable event ID propagated as the `event-id` Kafka header.

| Concern | Guarantee |
| --- | --- |
| Database transaction | Business change + outbox event = atomic |
| Broker publication | At least once |
| Consumer | Must handle duplicate event IDs |

This library does **not** advertise exactly-once delivery across PostgreSQL and Kafka.
That precision matters more in production than promising an impossible guarantee.

See [docs/delivery-guarantees.md](docs/delivery-guarantees.md).

---

## Requirements

- Java 21+
- Spring Boot 3.x
- Spring JDBC
- PostgreSQL
- Apache Kafka
- Flyway (or an equivalent way to apply the shipped schema)

Core persistence uses Spring JDBC on purpose. Outbox claiming needs precise SQL,
conditional updates, and short lock scopes. JPA is not required.

---

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.erick9125</groupId>
    <artifactId>spring-outbox-relay</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("io.github.erick9125:spring-outbox-relay:0.1.0")
```

The library deliberately keeps its dependency footprint small: it brings neither Kafka nor
Flyway onto your classpath, so adopting it cannot activate a migration tool you did not ask
for. Declare what you use yourself:

- `spring-boot-starter-jdbc`
- PostgreSQL driver
- `spring-kafka` — required only for the bundled Kafka adapter, and optional if you supply
  your own `MessageBrokerPublisher`
- Flyway (recommended, to apply the shipped schema)

The library ships Spring Boot auto-configuration. When a `DataSource` and a `KafkaTemplate`
are present, the publisher, relay, recovery job, and cleanup job are registered
automatically. Without a `KafkaTemplate` — and without any other `MessageBrokerPublisher`
bean — the publisher is still registered so events accumulate durably, and the relay and
its schedule stay off.

---

## Database schema

The migration ships under `classpath:db/outbox`, deliberately outside Flyway's default
`classpath:db/migration` location: a library migration sitting in the default location would
collide with your application's own `V1__…` and break startup. Add the location alongside
your own:

```yaml
spring:
  flyway:
    locations:
      - classpath:db/migration
      - classpath:db/outbox
```

Or create an equivalent table yourself:

```sql
CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(150) NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    destination VARCHAR(200) NOT NULL,
    partition_key VARCHAR(200),
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(30) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(200),
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX idx_outbox_event_polling
    ON outbox_event (status, available_at, created_at);

CREATE INDEX idx_outbox_event_recovery
    ON outbox_event (status, locked_at)
    WHERE status = 'PROCESSING';
```

Event lifecycle:

| Status | Meaning |
| --- | --- |
| `PENDING` | Ready to be claimed when `available_at <= now()` |
| `PROCESSING` | Claimed by a relay instance |
| `PUBLISHED` | Accepted by Kafka and locally confirmed |
| `FAILED` | Retry budget exhausted or permanent publication error |

---

## Configuration

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/orders
    username: orders
    password: secret
  kafka:
    bootstrap-servers: localhost:9092
  outbox:
    relay:
      enabled: true
      batch-size: 100
      poll-interval: 1s
      lock-timeout: 5m
      default-max-attempts: 5
      instance-id: order-service-1
      recovery-interval: 1m
      maintenance-batch-size: 500
      publish-timeout: 30s
      retry:
        initial-delay: 5s
        maximum-delay: 5m
        multiplier: 2.0
        jitter: 0.2
      cleanup:
        enabled: true
        retention: 7d
        interval: 1h
```

| Property | Default | Description |
| --- | --- | --- |
| `spring.outbox.relay.enabled` | `true` | Enables auto-configuration and schedulers |
| `spring.outbox.relay.batch-size` | `100` | Events claimed per poll |
| `spring.outbox.relay.poll-interval` | `1s` | Delay between relay polls |
| `spring.outbox.relay.lock-timeout` | `5m` | Age after which `PROCESSING` locks are recovered |
| `spring.outbox.relay.default-max-attempts` | `5` | Default retry budget for new events |
| `spring.outbox.relay.instance-id` | hostname + pid | Worker identity stored in `locked_by` |
| `spring.outbox.relay.recovery-interval` | `1m` | Delay between recovery runs |
| `spring.outbox.relay.maintenance-batch-size` | `500` | Rows per statement in the recovery and cleanup jobs |
| `spring.outbox.relay.publish-timeout` | `30s` | Deadline for a whole batch to be acknowledged. Keep it below `lock-timeout` |
| `spring.outbox.relay.retry.initial-delay` | `5s` | Base backoff delay |
| `spring.outbox.relay.retry.maximum-delay` | `5m` | Backoff ceiling |
| `spring.outbox.relay.cleanup.retention` | `7d` | Retention for `PUBLISHED` rows |

---

## Usage

### 1. Publish inside a business transaction

Inject `OutboxPublisher` and call it in the same `@Transactional` method that persists
your aggregate:

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxPublisher outboxPublisher;

    public OrderService(OrderRepository orderRepository, OutboxPublisher outboxPublisher) {
        this.orderRepository = orderRepository;
        this.outboxPublisher = outboxPublisher;
    }

    @Transactional
    public Order createOrder(CreateOrderCommand command) {
        Order order = orderRepository.save(
            Order.create(command.customerId(), command.total())
        );

        outboxPublisher.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId(order.getId().toString())
                .eventType("order.created")
                .eventVersion(1)
                .destination("orders.events")
                .partitionKey(order.getId().toString())
                .payload(OrderCreatedEvent.from(order))
                .header("correlation-id", command.correlationId())
                .build()
        );

        return order;
    }
}
```

If the order insert fails, the outbox insert rolls back with it.
If the outbox insert fails, the order rolls back with it.

`publish()` requires an active transaction and throws `IllegalStateException` when there is
none. That is deliberate: called outside a transaction the outbox row would commit on its own,
the atomicity this library exists for would be gone, and nothing would say so — the symptom
would surface much later as drift between the database and the broker.

### 2. Message contract

```java
OutboxMessage.builder()
    .aggregateType("ORDER")          // domain aggregate family
    .aggregateId(orderId)            // aggregate identity
    .eventType("order.created")      // event name / routing intent
    .eventVersion(1)                 // contract version
    .destination("orders.events")    // Kafka topic
    .partitionKey(orderId)           // keeps related events ordered in a partition
    .payload(eventObject)            // serialized with Jackson to JSONB
    .headers(Map.of("correlation-id", "abc"))
    .maxAttempts(5)                  // optional override
    .build();
```

Payloads are serialized with Jackson. Version your event classes deliberately through
`eventType` and `eventVersion` so consumers can evolve safely.

### 3. Kafka headers written by the relay

Each published record includes:

| Header | Purpose |
| --- | --- |
| `event-id` | Stable outbox UUID for consumer deduplication |
| `event-type` | Event name |
| `event-version` | Contract version |
| `aggregate-type` | Aggregate family |
| `aggregate-id` | Aggregate identity |

Any headers you attach with `.header(...)` or `.headers(...)` are delivered alongside these.
The five names above are reserved for relay metadata: a header of the same name is dropped
with a warning rather than delivered as a second value, so consumers deduplicating on
`event-id` can trust what they read.

### 4. Consumer guidance

Consumers should treat delivery as at-least-once:

```java
@KafkaListener(topics = "orders.events", groupId = "billing")
public void onOrderCreated(ConsumerRecord<String, String> record) {
    String eventId = header(record, "event-id");

    if (processedEventStore.exists(eventId)) {
        return;
    }

    billingService.handle(objectMapper.readValue(record.value(), OrderCreatedEvent.class));
    processedEventStore.save(eventId);
}
```

Use `partitionKey` on the producer side when related events for the same aggregate must
preserve order within a Kafka partition.

### 5. Trigger the relay manually

The scheduled job only calls `OutboxRelay.relayBatch()`. You can invoke the same API in
tests or operational tooling:

```java
RelayResult result = outboxRelay.relayBatch();
// result.claimed(), published(), rescheduled(), failed(), lockLost()
```

---

## Failure and recovery behavior

### Retryable Kafka failure

```text
publish fails
  → attempts++
  → compute next available_at with exponential backoff + jitter
  → status = PENDING
```

Typical retryable conditions include timeouts, broker unavailability, temporary network
errors, and throttling.

### Permanent failure

Invalid destinations, permanent serialization problems, and other non-retryable errors
move the row to `FAILED` with `last_error` populated for inspection.

### Process crash after claim

```text
status stays PROCESSING
  → lock-timeout elapses
  → recovery job returns the row to PENDING
  → another worker may claim it
```

This preserves durability and can produce duplicates if Kafka already accepted the
message. That is expected under at-least-once semantics.

### Lost claim

A claim is a lease, not ownership: a stalled instance can find that the row it claimed has
already been reclaimed and taken over. The three terminal updates are therefore fenced on
`locked_by`, so a late outcome cannot overwrite the new owner's:

```text
worker-a claims → stalls past lock-timeout
  → recovery returns the row to PENDING
  → worker-b claims and publishes
  → worker-a's update matches no rows and is discarded
```

Counted in `outbox.events.lock.lost` and `RelayResult.lockLost()`. A sustained rate means
batches are not completing within `lock-timeout`: raise it, or lower `batch-size`.

More detail: [docs/failure-scenarios.md](docs/failure-scenarios.md) and
[docs/concurrency.md](docs/concurrency.md).

---

## Observability

### Metrics

| Metric | Meaning |
| --- | --- |
| `outbox.events.created` | Events persisted |
| `outbox.events.claimed` | Events claimed by the relay |
| `outbox.events.published` | Successful publications |
| `outbox.events.rescheduled` | Retryable failures |
| `outbox.events.failed` | Permanent / exhausted failures |
| `outbox.events.lock.lost` | Claims reclaimed by another instance mid-flight |
| `outbox.events.recovered` | Abandoned locks recovered |
| `outbox.publication.duration` | Broker acknowledgement latency, tagged `result=success|failure` |
| `outbox.pending.count` | Current backlog |
| `outbox.oldest.pending.age` | Age of the oldest pending event |

Tags stay low-cardinality: `destination`, `event_type`, `result`.
High-cardinality values such as `event_id` or full error messages are not used as tags.

### Traces / observations

Spans are created for:

- `outbox.persist`
- `outbox.claim`
- `outbox.publish`
- `outbox.mark-published`

These integrate through Micrometer Observation and can export to OpenTelemetry when
your application is configured for it.

---

## Relationship to Spring Modulith

Spring Modulith already includes an event publication registry for incomplete
application-event publications.

Spring Outbox Relay is a focused alternative for teams that need:

- an explicit outbox table they can query and operate on directly
- ownership of schema, polling, retries, and broker adapters
- concurrent `SKIP LOCKED` claiming
- usable backlog metrics and failed-state inspection
- the pattern without adopting Spring Modulith module boundaries

It is not positioned as “better than Spring Modulith” without benchmarks or production
comparison. Choose the tool that matches the operational control you need.

See [docs/spring-modulith-comparison.md](docs/spring-modulith-comparison.md).

---

## Example application

The repository includes a minimal `order-service` that creates an order, writes
`order.created` to the outbox, relays it to Kafka, and exposes received events.

```bash
cd examples/order-service
docker compose up -d
cd ../..
./gradlew :examples:order-service:bootRun
```

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","total":49.90}'

curl http://localhost:8080/events
```

---

## Building and testing

```bash
./gradlew check
```

Integration tests use Testcontainers with real PostgreSQL and exercise claiming,
rollback, retries, recovery, and concurrent workers.

---

## Documentation

- [Delivery guarantees](docs/delivery-guarantees.md)
- [Concurrency model](docs/concurrency.md)
- [Failure scenarios](docs/failure-scenarios.md)
- [Spring Modulith comparison](docs/spring-modulith-comparison.md)

---

## Current scope

Spring Outbox Relay currently targets one job done well:

**Persist outbox events in PostgreSQL and publish them reliably to Kafka.**

Included:

- transactional persistence
- concurrent polling
- Kafka adapter
- retries, recovery, cleanup
- metrics and basic tracing hooks

Not included:

- RabbitMQ, Debezium/CDC, multi-database dialects
- sagas, event sourcing, inbound consumers
- web dashboard, Schema Registry, Avro/Protobuf
- exactly-once cross-system delivery

---

## License

Apache License 2.0
