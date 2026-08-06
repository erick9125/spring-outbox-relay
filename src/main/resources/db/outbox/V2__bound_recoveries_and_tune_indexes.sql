-- Recovery budget, separate from the publication retry budget.
--
-- An event whose processing kills the JVM used to loop forever: recovery returned it to PENDING
-- with attempts untouched, a worker claimed it, it killed the process again. Spending `attempts`
-- on recoveries instead would bound the loop but punish innocent events, because every deploy
-- interrupts in-flight publications and each interruption would burn a retry. Two counters keep
-- the two failure modes apart.
ALTER TABLE outbox_event
    ADD COLUMN recoveries INTEGER NOT NULL DEFAULT 0;

-- Index tuning.
--
-- Note for an already-populated table: these run inside Flyway's transaction and take a write
-- lock for the duration. On a large outbox_event, create the new indexes by hand with
-- CREATE INDEX CONCURRENTLY first, then let this migration find them already present.

-- The polling query is `WHERE status = 'PENDING' AND available_at <= now() ORDER BY created_at`.
-- The old index led on status, so available_at was a range predicate and created_at could not be
-- used for the ordering: every poll paid for a sort. It also indexed every row, and PUBLISHED rows
-- dominate the table until cleanup removes them. A partial index covers only the rows the relay
-- actually looks at and keeps them in the order it wants them.
DROP INDEX IF EXISTS idx_outbox_event_polling;

CREATE INDEX IF NOT EXISTS idx_outbox_event_pending
    ON outbox_event (available_at, created_at)
    WHERE status = 'PENDING';

-- The cleanup job filters on status = 'PUBLISHED' AND published_at < ?, which had no usable index
-- at all: it scanned every published row on every run.
CREATE INDEX IF NOT EXISTS idx_outbox_event_published
    ON outbox_event (published_at)
    WHERE status = 'PUBLISHED';

-- status is redundant in the key of an index already partial on status.
DROP INDEX IF EXISTS idx_outbox_event_recovery;

CREATE INDEX IF NOT EXISTS idx_outbox_event_processing
    ON outbox_event (locked_at)
    WHERE status = 'PROCESSING';
