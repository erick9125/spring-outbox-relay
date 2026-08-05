# Failure scenarios

## Kafka unavailable

```text
Publication fails
→ attempts++
→ compute next available_at
→ status = PENDING
```

Events remain durable in PostgreSQL and are retried after backoff.

## Permanent publish error

Invalid destination, permanent serialization failure, or corrupted payload:

```text
status = FAILED
last_error = reason
```

Failed events remain inspectable until an operator intervenes.

## Process crash after claim

```text
Event stays PROCESSING
→ lock-timeout elapses
→ recovery job sets PENDING
→ another worker claims it
```

Attempts are preserved. This can produce at-least-once duplicates if Kafka already
accepted the message before the crash.

## Stalled instance, not crashed

A long GC pause or a network stall is indistinguishable from a crash: after `lock-timeout`
the recovery job hands the row to someone else. The difference is that a stalled instance
eventually wakes up and tries to record an outcome for a row it no longer owns.

```text
worker-a claims → stalls
→ lock-timeout elapses
→ recovery job sets PENDING
→ worker-b claims and publishes
→ worker-a wakes up
→ its update matches no rows and is discarded
```

The terminal updates are fenced on `locked_by`, so worker-a cannot overwrite worker-b's
outcome. See [concurrency.md](concurrency.md). Watch `outbox.events.lock.lost`: a steady rate
means batches are not completing within `lock-timeout`.

## Business transaction rollback

If the surrounding Spring transaction rolls back, the outbox insert rolls back too.
No orphan event is left behind.
