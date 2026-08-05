package io.github.erick9125.outbox.relay;

import io.github.erick9125.outbox.domain.RelayResult;

public interface OutboxRelay {

  RelayResult relayBatch();
}
