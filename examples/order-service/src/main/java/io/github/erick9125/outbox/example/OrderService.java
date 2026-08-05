package io.github.erick9125.outbox.example;

import io.github.erick9125.outbox.api.OutboxMessage;
import io.github.erick9125.outbox.api.OutboxPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

  private final OrderRepository orderRepository;
  private final OutboxPublisher outboxPublisher;

  public OrderService(OrderRepository orderRepository, OutboxPublisher outboxPublisher) {
    this.orderRepository = orderRepository;
    this.outboxPublisher = outboxPublisher;
  }

  @Transactional
  public Order createOrder(CreateOrderRequest request) {
    Order order = orderRepository.save(Order.create(request.customerId(), request.total()));

    outboxPublisher.publish(
        OutboxMessage.builder()
            .aggregateType("ORDER")
            .aggregateId(order.id().toString())
            .eventType("order.created")
            .eventVersion(1)
            .destination("orders.events")
            .partitionKey(order.id().toString())
            .payload(OrderCreatedEvent.from(order))
            .build());

    return order;
  }
}
