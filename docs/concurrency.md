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

## Claims are leases, not ownership

Because the claim transaction commits before the broker call, nothing stops the recovery job
from reclaiming the row while its owner is still working. That is intentional — it is what
makes a crashed instance recoverable — but it means a slow instance can find that the row it
claimed is no longer its own.

So the three terminal transitions are fenced on the current owner:

```sql
UPDATE outbox_event
SET ...
WHERE id = :id
  AND status = 'PROCESSING'
  AND locked_by = :instance_id;
```

If the update matches no rows, the claim was lost and the relay leaves the row completely
alone rather than overwriting whatever the new owner decided. Without the fence, a stalled
instance coming back to life could mark an event `PUBLISHED` that another instance is still
publishing, reschedule an event that was already delivered, or mark `FAILED` a row that
succeeded.

Lost claims are counted in `outbox.events.lock.lost` and reported as `RelayResult.lockLost()`.
A sustained non-zero rate means batches are not finishing within `lock-timeout`: raise
`lock-timeout`, or lower `batch-size` so a batch completes inside it.

## Execution model note

The Kafka adapter waits for producer acknowledgement (`Future.get` with timeout).
In 0.1.0 this is acceptable for a dedicated, bounded relay loop. Document and
tune `batch-size` and poll interval for your throughput targets.
