package io.github.erick9125.outbox.broker;

import io.github.erick9125.outbox.domain.OutboxEvent;
import io.github.erick9125.outbox.domain.PublicationResult;

public interface MessageBrokerPublisher {

  PublicationResult publish(OutboxEvent event);
}
