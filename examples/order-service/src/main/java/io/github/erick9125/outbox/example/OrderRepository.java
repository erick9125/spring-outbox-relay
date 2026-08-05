package io.github.erick9125.outbox.example;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

  private final JdbcTemplate jdbcTemplate;
  private final Map<UUID, Order> cache = new ConcurrentHashMap<>();

  public OrderRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public Order save(Order order) {
    jdbcTemplate.update(
        """
                INSERT INTO orders (id, customer_id, total)
                VALUES (?, ?, ?)
                """,
        order.id(),
        order.customerId(),
        order.total());
    cache.put(order.id(), order);
    return order;
  }

  public Optional<Order> findById(UUID id) {
    return Optional.ofNullable(cache.get(id));
  }
}
