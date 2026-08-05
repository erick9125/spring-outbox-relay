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

## Business transaction rollback

If the surrounding Spring transaction rolls back, the outbox insert rolls back too.
No orphan event is left behind.
