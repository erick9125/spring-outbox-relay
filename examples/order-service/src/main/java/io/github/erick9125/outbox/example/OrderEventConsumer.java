package io.github.erick9125.outbox.example;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Component
@RestController
public class OrderEventConsumer {

  private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

  private final List<ReceivedEvent> received = new CopyOnWriteArrayList<>();

  @KafkaListener(topics = "orders.events", groupId = "order-service-demo")
  public void onMessage(ConsumerRecord<String, String> record) {
    String eventId = header(record, "event-id");
    String eventType = header(record, "event-type");
    ReceivedEvent event =
        new ReceivedEvent(
            eventId, eventType, record.key(), record.value(), record.partition(), record.offset());
    received.add(event);
    log.info(
        "Received event {} type={} partition={} offset={}",
        eventId,
        eventType,
        record.partition(),
        record.offset());
  }

  @GetMapping("/events")
  public List<ReceivedEvent> events() {
    return Collections.unmodifiableList(new ArrayList<>(received));
  }

  private static String header(ConsumerRecord<String, String> record, String key) {
    var header = record.headers().lastHeader(key);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  public record ReceivedEvent(
      String eventId, String eventType, String key, String payload, int partition, long offset) {}
}
