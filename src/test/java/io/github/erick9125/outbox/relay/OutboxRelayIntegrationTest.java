package io.github.erick9125.outbox.relay;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxStatus;
import io.github.erick9125.outbox.broker.MessageBrokerPublisher;
import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;
import io.github.erick9125.outbox.domain.RelayResult;
import io.github.erick9125.outbox.exception.PermanentPublicationException;
import io.github.erick9125.outbox.exception.RetryablePublicationException;
import io.github.erick9125.outbox.support.AbstractPostgresIntegrationTest;
import io.github.erick9125.outbox.support.OutboxTestSupport;
import io.github.erick9125.outbox.support.OutboxTestSupport.Fixture;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRelayIntegrationTest extends AbstractPostgresIntegrationTest {

  private Fixture fixture;

  @BeforeEach
  void setUp() {
    fixture = OutboxTestSupport.fixture(POSTGRES);
    fixture.jdbcTemplate().update("DELETE FROM outbox_event");
  }

  private static OutboxMessage message(String aggregateId) {
    return OutboxMessage.builder()
        .aggregateType("ORDER")
        .aggregateId(aggregateId)
        .eventType("order.created")
        .destination("orders.events")
        .payload(Map.of("ok", true))
        .build();
  }

  @Test
  void claimsOnlyPendingAvailableEventsAndRespectsBatchSize() {
    IntStream.range(0, 5)
        .forEach(
            i ->
                fixture.publish(
                    OutboxMessage.builder()
                        .aggregateType("ORDER")
                        .aggregateId("order-" + i)
                        .eventType("order.created")
                        .destination("orders.events")
                        .payload(Map.of("i", i))
                        .build()));

    List<OutboxEvent> claimed = fixture.repository().claimBatch(2, "worker-a");
    assertThat(claimed).hasSize(2);
    assertThat(claimed).allMatch(event -> event.status() == OutboxStatus.PROCESSING);
    assertThat(claimed).allMatch(event -> "worker-a".equals(event.lockedBy()));
  }

  @Test
  void concurrentWorkersDoNotClaimTheSameEvent() throws Exception {
    IntStream.range(0, 30)
        .forEach(
            i ->
                fixture.publish(
                    OutboxMessage.builder()
                        .aggregateType("ORDER")
                        .aggregateId("order-" + i)
                        .eventType("order.created")
                        .destination("orders.events")
                        .partitionKey("order-" + i)
                        .payload(Map.of("i", i))
                        .build()));

    Set<UUID> claimedIds = ConcurrentHashMap.newKeySet();
    List<UUID> duplicates = new CopyOnWriteArrayList<>();

    MessageBrokerPublisher publisher =
        OutboxTestSupport.sync(
            event -> {
              if (!claimedIds.add(event.id())) {
                duplicates.add(event.id());
              }
              return new PublicationResult("msg-" + event.id(), Instant.now());
            });

    OutboxRelay workerA = OutboxTestSupport.relay(fixture, publisher, "worker-a");
    OutboxRelay workerB = OutboxTestSupport.relay(fixture, publisher, "worker-b");
    OutboxRelay workerC = OutboxTestSupport.relay(fixture, publisher, "worker-c");

    java.util.concurrent.CompletableFuture.allOf(
            java.util.concurrent.CompletableFuture.runAsync(workerA::relayBatch),
            java.util.concurrent.CompletableFuture.runAsync(workerB::relayBatch),
            java.util.concurrent.CompletableFuture.runAsync(workerC::relayBatch))
        .join();

    while (fixture.repository().countPending() > 0) {
      java.util.concurrent.CompletableFuture.allOf(
              java.util.concurrent.CompletableFuture.runAsync(workerA::relayBatch),
              java.util.concurrent.CompletableFuture.runAsync(workerB::relayBatch),
              java.util.concurrent.CompletableFuture.runAsync(workerC::relayBatch))
          .join();
    }

    assertThat(duplicates).isEmpty();
    assertThat(claimedIds).hasSize(30);
    assertThat(fixture.repository().countPending()).isZero();
  }

  @Test
  void handsTheWholeBatchToTheBrokerBeforeAwaitingAnyOfIt() {
    // The relay used to await each acknowledgement before sending the next, so a batch cost the sum
    // of its latencies. Every event must now be in flight before the first one is settled: this
    // publisher refuses to complete until it has seen all five.
    int batch = 5;
    IntStream.range(0, batch).forEach(i -> fixture.publish(message("parallel-" + i)));

    CountDownLatch allSent = new CountDownLatch(batch);
    ExecutorService acks = Executors.newFixedThreadPool(batch);
    try {
      OutboxRelay relay =
          OutboxTestSupport.relay(
              fixture,
              event -> {
                allSent.countDown();
                return CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        // Deadlocks unless the other four were handed over first.
                        if (!allSent.await(10, TimeUnit.SECONDS)) {
                          throw new IllegalStateException("batch was published serially");
                        }
                      } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(exception);
                      }
                      return new PublicationResult("broker-" + event.id(), Instant.now());
                    },
                    acks);
              },
              "worker-a");

      RelayResult result = relay.relayBatch();

      assertThat(result.claimed()).isEqualTo(batch);
      assertThat(result.published()).isEqualTo(batch);
      assertThat(result.failed()).isZero();
      assertThat(result.rescheduled()).isZero();
    } finally {
      acks.shutdownNow();
    }
  }

  @Test
  void appliesThePublishTimeoutToTheBatchRatherThanToEachEvent() {
    // A broker that never acknowledges: with a per-event timeout, three events would take three
    // timeouts. One deadline for the whole batch keeps a stuck broker from holding the claims for
    // longer than publish-timeout, which is what let lock-timeout elapse mid-batch.
    IntStream.range(0, 3).forEach(i -> fixture.publish(message("stuck-" + i)));

    OutboxRelay relay =
        OutboxTestSupport.relayWithPublishTimeout(
            fixture, event -> new CompletableFuture<>(), "worker-a", Duration.ofMillis(300));

    Instant start = Instant.now();
    RelayResult result = relay.relayBatch();
    Duration elapsed = Duration.between(start, Instant.now());

    assertThat(result.claimed()).isEqualTo(3);
    assertThat(result.rescheduled()).isEqualTo(3);
    assertThat(result.published()).isZero();
    // Three per-event timeouts would be 900ms or more; one shared deadline stays near 300ms.
    assertThat(elapsed).isLessThan(Duration.ofMillis(800));

    // A timeout is retryable: the rows go back to PENDING with the reason recorded, because the
    // send
    // may still land and the consumer deduplicates.
    assertThat(fixture.repository().countPending()).isEqualTo(3);
    assertThat(
            fixture
                .jdbcTemplate()
                .queryForObject("SELECT last_error FROM outbox_event LIMIT 1", String.class))
        .contains("did not acknowledge");
  }

  @Test
  void publishesAndMarksPublished() {
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("order-9")
                .eventType("order.created")
                .destination("orders.events")
                .partitionKey("order-9")
                .payload(Map.of("total", 10))
                .build());

    List<OutboxEvent> publishedEvents = new CopyOnWriteArrayList<>();
    OutboxRelay relay =
        OutboxTestSupport.relay(
            fixture,
            OutboxTestSupport.sync(
                event -> {
                  publishedEvents.add(event);
                  return new PublicationResult("broker-1", Instant.now());
                }),
            "worker-a");

    RelayResult result = relay.relayBatch();
    assertThat(result.published()).isEqualTo(1);
    assertThat(publishedEvents).extracting(OutboxEvent::id).containsExactly(id);
    assertThat(fixture.repository().findById(id).orElseThrow().status())
        .isEqualTo(OutboxStatus.PUBLISHED);
  }

  @Test
  void reschedulesRetryableFailuresAndFailsPermanentOnes() {
    UUID retryableId =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("retryable")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("type", "retryable"))
                .maxAttempts(3)
                .build());
    UUID permanentId =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("permanent")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("type", "permanent"))
                .maxAttempts(3)
                .build());

    OutboxRelay relay =
        OutboxTestSupport.relay(
            fixture,
            OutboxTestSupport.sync(
                event -> {
                  if ("retryable".equals(event.aggregateId())) {
                    throw new RetryablePublicationException("broker unavailable");
                  }
                  throw new PermanentPublicationException("invalid destination");
                }),
            "worker-a");

    RelayResult result = relay.relayBatch();
    assertThat(result.rescheduled() + result.failed()).isEqualTo(2);

    OutboxEvent retryable = fixture.repository().findById(retryableId).orElseThrow();
    OutboxEvent permanent = fixture.repository().findById(permanentId).orElseThrow();

    assertThat(retryable.status()).isEqualTo(OutboxStatus.PENDING);
    assertThat(retryable.attempts()).isEqualTo(1);
    assertThat(retryable.availableAt()).isAfter(retryable.createdAt());
    assertThat(permanent.status()).isEqualTo(OutboxStatus.FAILED);
    assertThat(permanent.attempts()).isEqualTo(1);
  }

  @Test
  void doesNotClaimFailedOrPublishedEvents() {
    UUID pending =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("pending")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("s", "pending"))
                .build());
    UUID published =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("published")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("s", "published"))
                .build());
    UUID failed =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("failed")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("s", "failed"))
                .build());

    // The terminal transitions are fenced on the current owner, so the rows have to be claimed
    // before they can be moved out of PENDING.
    fixture.repository().claimBatch(10, "setup-worker");
    assertThat(fixture.repository().markPublished(published, "setup-worker", Instant.now(), "b-1"))
        .isTrue();
    assertThat(fixture.repository().markFailed(failed, "setup-worker", 5, "exhausted")).isTrue();
    assertThat(
            fixture
                .repository()
                .reschedule(
                    pending, "setup-worker", 0, Instant.now().minus(Duration.ofSeconds(1)), null))
        .isTrue();

    List<OutboxEvent> claimed = fixture.repository().claimBatch(10, "worker-a");
    assertThat(claimed.stream().map(OutboxEvent::id).collect(Collectors.toSet()))
        .containsExactly(pending);
  }

  @Test
  void leavesTheRowAloneWhenTheClaimWasLostBeforeItCouldBeSettled() {
    UUID id =
        fixture.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId("stolen")
                .eventType("order.created")
                .destination("orders.events")
                .payload(Map.of("s", "stolen"))
                .build());

    // worker-a claims the event, then stalls long enough for recovery to reclaim it.
    List<OutboxEvent> claimedByA = fixture.repository().claimBatch(1, "worker-a");
    assertThat(claimedByA).hasSize(1);
    fixture.repository().recoverAbandoned(Instant.now().plus(Duration.ofMinutes(1)), 100);

    // worker-b now owns the row and publishes it.
    OutboxRelay workerB =
        OutboxTestSupport.relay(
            fixture,
            OutboxTestSupport.sync(event -> new PublicationResult("broker-b", Instant.now())),
            "worker-b");
    assertThat(workerB.relayBatch().published()).isEqualTo(1);

    OutboxEvent afterB = fixture.repository().findById(id).orElseThrow();

    // worker-a finally comes back. Its outcome must not touch the row.
    assertThat(fixture.repository().markPublished(id, "worker-a", Instant.now(), "broker-a"))
        .isFalse();
    assertThat(fixture.repository().markFailed(id, "worker-a", 9, "stale failure")).isFalse();
    assertThat(
            fixture
                .repository()
                .reschedule(id, "worker-a", 9, Instant.now().plus(Duration.ofHours(1)), "stale"))
        .isFalse();

    OutboxEvent afterA = fixture.repository().findById(id).orElseThrow();
    assertThat(afterA).isEqualTo(afterB);
    assertThat(afterA.status()).isEqualTo(OutboxStatus.PUBLISHED);
    assertThat(afterA.headers()).contains("broker-b");
  }

  @Test
  void reportsLockLossInsteadOfRepublishingWhenTheClaimIsGone() {
    fixture.publish(
        OutboxMessage.builder()
            .aggregateType("ORDER")
            .aggregateId("hijacked")
            .eventType("order.created")
            .destination("orders.events")
            .payload(Map.of("s", "hijacked"))
            .build());

    // The claim is taken away between the claim and the publication, which is what a stalled
    // instance experiences after lock-timeout elapses.
    OutboxRelay workerA =
        OutboxTestSupport.relay(
            fixture,
            OutboxTestSupport.sync(
                event -> {
                  fixture
                      .repository()
                      .recoverAbandoned(Instant.now().plus(Duration.ofMinutes(1)), 100);
                  fixture.repository().claimBatch(1, "worker-b");
                  return new PublicationResult("broker-a", Instant.now());
                }),
            "worker-a");

    RelayResult result = workerA.relayBatch();

    assertThat(result.claimed()).isEqualTo(1);
    assertThat(result.lockLost()).isEqualTo(1);
    assertThat(result.published()).isZero();
    assertThat(result.rescheduled()).isZero();
    assertThat(result.failed()).isZero();
  }
}
