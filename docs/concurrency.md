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

## Execution model

The relay hands the whole claimed batch to the broker before awaiting any of it, then settles
each event against a single deadline — `publish-timeout`, 30s by default.

```text
claim batch (short transaction, commit)
→ hand every event to the broker      (no waiting)
→ await each acknowledgement          (one deadline for the batch)
→ mark PUBLISHED / reschedule / FAILED
```

This matters for more than throughput. Awaiting each acknowledgement before sending the next
made a batch cost the *sum* of its latencies: with `batch-size: 100` and a stuck broker that is
100 timeouts back to back, far longer than the 5 minute `lock-timeout`. The recovery job would
start reclaiming rows the relay was still publishing, which shows up as a flood of
`outbox.events.lock.lost`.

So the two settings are related: **keep `publish-timeout` below `lock-timeout`**. The first
bounds how long a poll can hold its claims; the second is when someone else may take them.

`MessageBrokerPublisher` returns a `CompletableFuture` for this reason. An adapter that blocks
inside `publish` puts the serial chain back.

Each job runs on its own thread in a pool the library owns, so a cleanup run deleting a large
backlog cannot delay the relay. That pool is not published as a bean: a second `TaskScheduler`
in the context would change which one the application's own `@Scheduled` methods resolve to.

## No ordering guarantee

**Events are not delivered in order.** Not globally, and not per aggregate.

```text
Two events for order-1, published in sequence:

instance A claims #1 ─┐
instance B claims #2 ─┴─► both publish concurrently → either order
```

Three separate reasons, any one of which is enough:

- `SKIP LOCKED` hands different rows to different instances, which publish in parallel.
- Within one batch, every event is handed to the broker at once, so acknowledgements interleave.
- A failure reorders regardless of the above: if #1 is rescheduled 5 seconds out while #2
  publishes immediately, #2 arrives first — even with a single instance and a single thread.

`partitionKey` decides which Kafka partition an event lands in. That bounds where ordering
*could* hold, but it does not create it: the relay never serialises the events sharing a key.

If your consumers need order, they have to handle it themselves — sequence numbers per
aggregate, or a design where the events are commutative. Version your payloads with
`eventVersion` and include whatever ordering data the consumer needs inside the event.
