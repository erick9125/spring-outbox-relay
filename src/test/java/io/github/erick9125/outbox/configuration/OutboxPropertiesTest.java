package io.github.erick9125.outbox.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.erick9125.outbox.support.OutboxTestSupport;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class OutboxPropertiesTest {

  @Test
  void rejectsNonPositiveCountsInsteadOfSubstitutingADefault() {
    // Quietly correcting these meant a typo in a deployment behaved like a working configuration,
    // with no way for the operator to tell that the value they set was being ignored.
    assertThatThrownBy(() -> OutboxTestSupport.props().batchSize(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("spring.outbox.relay.batch-size")
        .hasMessageContaining("was 0");

    assertThatThrownBy(() -> OutboxTestSupport.props().defaultMaxAttempts(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("default-max-attempts");

    assertThatThrownBy(() -> OutboxTestSupport.props().maintenanceBatchSize(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maintenance-batch-size");

    assertThatThrownBy(() -> OutboxTestSupport.props().maxRecoveries(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("max-recoveries");
  }

  @Test
  void rejectsNonPositiveDurations() {
    assertThatThrownBy(() -> OutboxTestSupport.props().pollInterval(Duration.ZERO).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("poll-interval");

    assertThatThrownBy(
            () -> OutboxTestSupport.props().publishTimeout(Duration.ofSeconds(-1)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("publish-timeout");

    assertThatThrownBy(() -> OutboxTestSupport.props().lockTimeout(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lock-timeout");
  }

  @Test
  void allowsAZeroShutdownTimeoutForCallersThatDoNotWantToWait() {
    assertThat(OutboxTestSupport.props().shutdownTimeout(Duration.ZERO).build().shutdownTimeout())
        .isZero();

    assertThatThrownBy(
            () -> OutboxTestSupport.props().shutdownTimeout(Duration.ofSeconds(-1)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("shutdown-timeout");
  }

  @Test
  void fillsInTheNestedGroupsWhenAbsent() {
    OutboxProperties properties = OutboxProperties.defaults();

    assertThat(properties.retry()).isEqualTo(OutboxProperties.Retry.defaults());
    assertThat(properties.cleanup()).isEqualTo(OutboxProperties.Cleanup.defaults());
    assertThat(properties.maxRecoveries()).isEqualTo(3);
    assertThat(properties.publishTimeout()).isEqualTo(Duration.ofSeconds(30));
  }
}
