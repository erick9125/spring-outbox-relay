package io.github.erick9125.outbox.scheduling;

import io.github.erick9125.outbox.cleanup.OutboxCleanupService;
import io.github.erick9125.outbox.configuration.OutboxProperties;
import io.github.erick9125.outbox.observability.OutboxMetrics;
import io.github.erick9125.outbox.recovery.OutboxRecoveryService;
import io.github.erick9125.outbox.relay.OutboxRelay;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Runs the relay, recovery, cleanup and backlog-metrics jobs on a thread pool of its own.
 *
 * <p>These used to be {@code @Scheduled} methods, which meant two things. The library had to turn
 * on {@code @EnableScheduling} for the entire host application — a side effect nobody asked for —
 * and all four jobs shared Spring Boot's default scheduler, whose pool size is one. A cleanup run
 * deleting a large backlog therefore blocked the relay that is supposed to poll every second.
 *
 * <p>The pool is deliberately not exposed as a bean. A second {@code TaskScheduler} in the context
 * can change which one the application's own {@code @Scheduled} methods resolve to, and this
 * library has no business affecting that. Pass one in if you want to supply your own.
 *
 * <p>Shutdown is graceful: {@link #stop()} stops scheduling further runs and then waits up to
 * {@code shutdown-timeout} for the run in progress. Without it, every deploy killed the relay
 * mid-publication and left up to {@code batch-size} rows sitting in {@code PROCESSING} until {@code
 * lock-timeout} elapsed — minutes of delay on every release.
 */
public final class OutboxScheduler implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);

  /** One thread per job, so none of them can delay another. */
  private static final int POOL_SIZE = 4;

  private final OutboxRelay outboxRelay;
  private final OutboxRecoveryService recoveryService;
  private final OutboxCleanupService cleanupService;
  private final OutboxMetrics metrics;
  private final OutboxProperties properties;
  private final ThreadPoolTaskScheduler taskScheduler;

  private final List<ScheduledFuture<?>> scheduled = new CopyOnWriteArrayList<>();
  private volatile boolean running;

  public OutboxScheduler(
      OutboxRelay outboxRelay,
      OutboxRecoveryService recoveryService,
      OutboxCleanupService cleanupService,
      OutboxMetrics metrics,
      OutboxProperties properties) {
    this.outboxRelay = Objects.requireNonNull(outboxRelay);
    this.recoveryService = Objects.requireNonNull(recoveryService);
    this.cleanupService = Objects.requireNonNull(cleanupService);
    this.metrics = Objects.requireNonNull(metrics);
    this.properties = Objects.requireNonNull(properties);
    this.taskScheduler = createTaskScheduler(properties);
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    taskScheduler.initialize();
    scheduled.add(schedule("relay", outboxRelay::relayBatch, properties.pollInterval()));
    scheduled.add(
        schedule(
            "recovery", recoveryService::recoverAbandonedEvents, properties.recoveryInterval()));
    scheduled.add(
        schedule(
            "cleanup", cleanupService::cleanupPublishedEvents, properties.cleanup().interval()));
    scheduled.add(
        schedule("backlog-metrics", metrics::refreshBacklog, properties.backlogMetricsInterval()));
    running = true;
    log.info(
        "Outbox scheduler started: poll={}, recovery={}, cleanup={}, backlog-metrics={}",
        properties.pollInterval(),
        properties.recoveryInterval(),
        properties.cleanup().interval(),
        properties.backlogMetricsInterval());
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    running = false;
    // false, not true: let the run in progress finish so its events are settled and their claims
    // released, instead of abandoning them to lock-timeout.
    scheduled.forEach(task -> task.cancel(false));
    scheduled.clear();
    taskScheduler.shutdown();
    log.info("Outbox scheduler stopped");
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private ScheduledFuture<?> schedule(String job, Runnable task, java.time.Duration interval) {
    return taskScheduler.scheduleWithFixedDelay(guard(job, task), interval);
  }

  /**
   * Keeps one bad run from unscheduling the job. A raw {@code ScheduledExecutorService} drops a
   * repeating task the moment it throws, and a relay that stops polling because the database
   * blipped once is worse than a noisy log.
   */
  private static Runnable guard(String job, Runnable task) {
    return () -> {
      try {
        task.run();
      } catch (Exception exception) {
        log.error("Outbox {} job failed; it will run again on the next interval", job, exception);
      }
    };
  }

  private static ThreadPoolTaskScheduler createTaskScheduler(OutboxProperties properties) {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(POOL_SIZE);
    scheduler.setThreadNamePrefix("outbox-");
    scheduler.setWaitForTasksToCompleteOnShutdown(true);
    scheduler.setAwaitTerminationSeconds(Math.toIntExact(properties.shutdownTimeout().toSeconds()));
    return scheduler;
  }
}
