package io.github.erick9125.outbox.persistence;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.domain.OutboxEvent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcOutboxRepository implements OutboxRepository {

  private static final RowMapper<OutboxEvent> ROW_MAPPER = JdbcOutboxRepository::mapRow;

  private final JdbcTemplate jdbcTemplate;
  private final OutboxProperties properties;
  private final Clock clock;
  private final TransactionTemplate transactionTemplate;

  public JdbcOutboxRepository(JdbcTemplate jdbcTemplate, OutboxProperties properties) {
    this(jdbcTemplate, properties, Clock.systemUTC(), null);
  }

  public JdbcOutboxRepository(JdbcTemplate jdbcTemplate, OutboxProperties properties, Clock clock) {
    this(jdbcTemplate, properties, clock, null);
  }

  public JdbcOutboxRepository(
      JdbcTemplate jdbcTemplate,
      OutboxProperties properties,
      Clock clock,
      PlatformTransactionManager transactionManager) {
    this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
    this.properties = Objects.requireNonNull(properties);
    this.clock = Objects.requireNonNull(clock);
    PlatformTransactionManager manager =
        transactionManager != null
            ? transactionManager
            : createTransactionManager(jdbcTemplate.getDataSource());
    this.transactionTemplate = new TransactionTemplate(manager);
  }

  @Override
  public UUID insert(
      OutboxMessage message, String payloadJson, String headersJson, Instant occurredAt) {
    UUID id = UUID.randomUUID();
    Instant now = clock.instant();
    int maxAttempts =
        message.maxAttempts() != null ? message.maxAttempts() : properties.defaultMaxAttempts();

    jdbcTemplate.update(
        """
                INSERT INTO outbox_event (
                    id, aggregate_type, aggregate_id, event_type, event_version,
                    destination, partition_key, payload, headers,
                    status, attempts, max_attempts,
                    occurred_at, created_at, available_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, 0, ?, ?, ?, ?)
                """,
        id,
        message.aggregateType(),
        message.aggregateId(),
        message.eventType(),
        message.eventVersion(),
        message.destination(),
        message.partitionKey(),
        payloadJson,
        headersJson,
        OutboxStatus.PENDING.name(),
        maxAttempts,
        Timestamp.from(occurredAt),
        Timestamp.from(now),
        Timestamp.from(now));
    return id;
  }

  @Override
  public List<OutboxEvent> claimBatch(int batchSize, String lockedBy) {
    List<OutboxEvent> claimed =
        transactionTemplate.execute(status -> doClaimBatch(batchSize, lockedBy));
    return claimed == null ? List.of() : claimed;
  }

  /**
   * Selects, locks and claims a batch in a single statement.
   *
   * <p>This used to be a SELECT, then one UPDATE per row, then a SELECT to read the rows back — 2 +
   * batch-size round trips every poll, 102 of them per second at the default settings, all while
   * holding row locks. The CTE does the same work atomically: {@code FOR UPDATE SKIP LOCKED} still
   * hands disjoint batches to concurrent workers, and {@code RETURNING} gives back the rows the
   * update touched.
   *
   * <p>{@code RETURNING} has no defined order, so the batch is sorted afterwards to keep the
   * roughly-FIFO delivery order the previous {@code ORDER BY created_at} produced.
   */
  private List<OutboxEvent> doClaimBatch(int batchSize, String lockedBy) {
    Instant now = clock.instant();
    List<OutboxEvent> claimed =
        jdbcTemplate.query(
            """
                WITH due AS (
                    SELECT id
                    FROM outbox_event
                    WHERE status = ?
                      AND available_at <= ?
                    ORDER BY available_at, created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT ?
                )
                UPDATE outbox_event o
                SET status = ?,
                    locked_at = ?,
                    locked_by = ?
                FROM due
                WHERE o.id = due.id
                RETURNING o.*
                """,
            ROW_MAPPER,
            OutboxStatus.PENDING.name(),
            Timestamp.from(now),
            batchSize,
            OutboxStatus.PROCESSING.name(),
            Timestamp.from(now),
            lockedBy);

    return claimed.stream().sorted(Comparator.comparing(OutboxEvent::createdAt)).toList();
  }

  @Override
  public boolean markPublished(
      UUID id, String lockedBy, Instant publishedAt, String brokerMessageId) {
    // jsonb_set is strict: a NULL new_value collapses the whole expression to NULL, and headers is
    // NOT NULL. Without the COALESCE, a broker adapter that reports no message id would fail this
    // update, the relay would treat the failure as retryable, and an already-published event would
    // be sent again on every attempt until its budget ran out.
    int updated =
        jdbcTemplate.update(
            """
                UPDATE outbox_event
                SET status = ?,
                    published_at = ?,
                    locked_at = NULL,
                    locked_by = NULL,
                    last_error = NULL,
                    headers = jsonb_set(
                        COALESCE(headers, '{}'::jsonb),
                        '{brokerMessageId}',
                        COALESCE(to_jsonb(?::text), 'null'::jsonb),
                        true
                    )
                WHERE id = ?
                  AND status = ?
                  AND locked_by = ?
                """,
            OutboxStatus.PUBLISHED.name(),
            Timestamp.from(publishedAt),
            brokerMessageId,
            id,
            OutboxStatus.PROCESSING.name(),
            lockedBy);
    return updated > 0;
  }

  @Override
  public boolean reschedule(
      UUID id, String lockedBy, int attempts, Instant availableAt, String lastError) {
    int updated =
        jdbcTemplate.update(
            """
                UPDATE outbox_event
                SET status = ?,
                    attempts = ?,
                    available_at = ?,
                    locked_at = NULL,
                    locked_by = NULL,
                    last_error = ?
                WHERE id = ?
                  AND status = ?
                  AND locked_by = ?
                """,
            OutboxStatus.PENDING.name(),
            attempts,
            Timestamp.from(availableAt),
            truncate(lastError),
            id,
            OutboxStatus.PROCESSING.name(),
            lockedBy);
    return updated > 0;
  }

  @Override
  public boolean markFailed(UUID id, String lockedBy, int attempts, String lastError) {
    int updated =
        jdbcTemplate.update(
            """
                UPDATE outbox_event
                SET status = ?,
                    attempts = ?,
                    locked_at = NULL,
                    locked_by = NULL,
                    last_error = ?
                WHERE id = ?
                  AND status = ?
                  AND locked_by = ?
                """,
            OutboxStatus.FAILED.name(),
            attempts,
            truncate(lastError),
            id,
            OutboxStatus.PROCESSING.name(),
            lockedBy);
    return updated > 0;
  }

  @Override
  public int recoverAbandoned(Instant lockedBefore, int maxRecoveries, int limit) {
    return jdbcTemplate.update(
        """
                UPDATE outbox_event
                SET status = ?,
                    locked_at = NULL,
                    locked_by = NULL,
                    available_at = ?,
                    recoveries = recoveries + 1
                WHERE id IN (
                    SELECT id
                    FROM outbox_event
                    WHERE status = ?
                      AND locked_at < ?
                      AND recoveries < ?
                    ORDER BY locked_at
                    LIMIT ?
                )
                """,
        OutboxStatus.PENDING.name(),
        Timestamp.from(clock.instant()),
        OutboxStatus.PROCESSING.name(),
        Timestamp.from(lockedBefore),
        maxRecoveries,
        limit);
  }

  @Override
  public int failExhaustedRecoveries(Instant lockedBefore, int maxRecoveries, int limit) {
    return jdbcTemplate.update(
        """
                UPDATE outbox_event
                SET status = ?,
                    locked_at = NULL,
                    locked_by = NULL,
                    last_error = ?
                WHERE id IN (
                    SELECT id
                    FROM outbox_event
                    WHERE status = ?
                      AND locked_at < ?
                      AND recoveries >= ?
                    ORDER BY locked_at
                    LIMIT ?
                )
                """,
        OutboxStatus.FAILED.name(),
        "recovery budget exhausted after " + maxRecoveries + " abandoned claims",
        OutboxStatus.PROCESSING.name(),
        Timestamp.from(lockedBefore),
        maxRecoveries,
        limit);
  }

  @Override
  public int deletePublishedBefore(Instant publishedBefore, int limit) {
    return jdbcTemplate.update(
        """
                DELETE FROM outbox_event
                WHERE id IN (
                    SELECT id
                    FROM outbox_event
                    WHERE status = ?
                      AND published_at < ?
                    ORDER BY published_at
                    LIMIT ?
                )
                """,
        OutboxStatus.PUBLISHED.name(),
        Timestamp.from(publishedBefore),
        limit);
  }

  @Override
  public long countPending() {
    Long count =
        jdbcTemplate.queryForObject(
            """
                        SELECT COUNT(*)
                        FROM outbox_event
                        WHERE status = ?
                        """,
            Long.class,
            OutboxStatus.PENDING.name());
    return count == null ? 0L : count;
  }

  @Override
  public Optional<Instant> findOldestPendingAvailableAt() {
    List<Instant> values =
        jdbcTemplate.query(
            """
                        SELECT available_at
                        FROM outbox_event
                        WHERE status = ?
                        ORDER BY available_at
                        LIMIT 1
                        """,
            (rs, rowNum) -> rs.getTimestamp("available_at").toInstant(),
            OutboxStatus.PENDING.name());
    return values.stream().findFirst();
  }

  @Override
  public Optional<OutboxEvent> findById(UUID id) {
    List<OutboxEvent> events =
        jdbcTemplate.query(
            """
                        SELECT *
                        FROM outbox_event
                        WHERE id = ?
                        """,
            ROW_MAPPER,
            id);
    return events.stream().findFirst();
  }

  private static PlatformTransactionManager createTransactionManager(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalStateException("JdbcTemplate must be backed by a DataSource");
    }
    return new DataSourceTransactionManager(dataSource);
  }

  private static OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new OutboxEvent(
        (UUID) rs.getObject("id"),
        rs.getString("aggregate_type"),
        rs.getString("aggregate_id"),
        rs.getString("event_type"),
        rs.getInt("event_version"),
        rs.getString("destination"),
        rs.getString("partition_key"),
        rs.getString("payload"),
        rs.getString("headers"),
        OutboxStatus.valueOf(rs.getString("status")),
        rs.getInt("attempts"),
        rs.getInt("max_attempts"),
        rs.getInt("recoveries"),
        rs.getTimestamp("occurred_at").toInstant(),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("available_at").toInstant(),
        optionalInstant(rs, "locked_at"),
        rs.getString("locked_by"),
        optionalInstant(rs, "published_at"),
        rs.getString("last_error"));
  }

  private static Instant optionalInstant(ResultSet rs, String column) throws SQLException {
    Timestamp timestamp = rs.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
  }

  private static String truncate(String value) {
    if (value == null) {
      return null;
    }
    return value.length() <= 4000 ? value : value.substring(0, 4000);
  }
}
