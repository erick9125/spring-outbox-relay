# Concurrency model

Multiple relay instances can poll at the same time.

## Claim strategy

```sql
SELECT id
FROM outbox_event
WHERE status = 'PENDING'
  AND available_at <= NOW()
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

Then mark rows as `PROCESSING` and commit.

`SKIP LOCKED` lets workers claim disjoint batches without waiting on the same rows.

## Short transactions only

Correct:

```text
open transaction
→ select + lock
→ mark PROCESSING
→ commit
→ publish to Kafka
→ mark PUBLISHED / reschedule / FAILED
```

Incorrect:

```text
open transaction
→ lock rows
→ publish to Kafka for several seconds
→ commit
```

Holding PostgreSQL row locks during broker I/O increases contention and failure blast radius.

## Execution model note

The Kafka adapter waits for producer acknowledgement (`Future.get` with timeout).
In 0.1.0 this is acceptable for a dedicated, bounded relay loop. Document and
tune `batch-size` and poll interval for your throughput targets.
