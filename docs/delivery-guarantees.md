# Delivery guarantees

Spring Outbox Relay promises **reliable at-least-once event publication**.

## What is atomic

```text
Database transaction:
Business change + outbox event = atomic
```

If either insert fails, both roll back.

## What is at-least-once

```text
Broker publication:
At least once
```

A failure after Kafka accepts the message and before the outbox row is marked
`PUBLISHED` can cause the relay to publish again.

## Consumer responsibility

```text
Consumer:
Must handle duplicate event IDs
```

Every published Kafka record includes a stable `event-id` header. Consumers should
deduplicate on that identifier.

## Why not exactly-once

Although Spring Kafka supports transactional / EOS patterns for some Kafka-to-Kafka
flows, an arbitrary PostgreSQL business transaction plus a Kafka publish is not
automatically a single distributed transaction.

Advertising exactly-once here would be inaccurate.
