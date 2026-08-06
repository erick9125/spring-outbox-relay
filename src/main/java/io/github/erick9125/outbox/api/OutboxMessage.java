package io.github.erick9125.outbox.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record OutboxMessage(
    String aggregateType,
    String aggregateId,
    String eventType,
    int eventVersion,
    String destination,
    String partitionKey,
    Object payload,
    Map<String, String> headers,
    Integer maxAttempts) {

  public OutboxMessage {
    Objects.requireNonNull(aggregateType, "aggregateType must not be null");
    Objects.requireNonNull(aggregateId, "aggregateId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(destination, "destination must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
    if (eventVersion < 1) {
      throw new IllegalArgumentException("eventVersion must be >= 1");
    }
    if (maxAttempts != null && maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1");
    }

    // Checked against the column widths here, where the message names the field. Left to the
    // insert,
    // an over-long value surfaced as a raw SQL error that took the caller's business transaction
    // down with it.
    requireFits(aggregateType, "aggregateType", 100);
    requireFits(aggregateId, "aggregateId", 150);
    requireFits(eventType, "eventType", 150);
    requireFits(destination, "destination", 200);
    requireFits(partitionKey, "partitionKey", 200);
    headers =
        headers == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
  }

  private static void requireFits(String value, String field, int maxLength) {
    if (value != null && value.length() > maxLength) {
      throw new IllegalArgumentException(
          field
              + " must be at most "
              + maxLength
              + " characters to fit the outbox_event column, was "
              + value.length());
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private int eventVersion = 1;
    private String destination;
    private String partitionKey;
    private Object payload;
    private Map<String, String> headers = new LinkedHashMap<>();
    private Integer maxAttempts;

    public Builder aggregateType(String aggregateType) {
      this.aggregateType = aggregateType;
      return this;
    }

    public Builder aggregateId(String aggregateId) {
      this.aggregateId = aggregateId;
      return this;
    }

    public Builder eventType(String eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder eventVersion(int eventVersion) {
      this.eventVersion = eventVersion;
      return this;
    }

    public Builder destination(String destination) {
      this.destination = destination;
      return this;
    }

    public Builder partitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder payload(Object payload) {
      this.payload = payload;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers = headers == null ? new LinkedHashMap<>() : new LinkedHashMap<>(headers);
      return this;
    }

    public Builder header(String key, String value) {
      this.headers.put(key, value);
      return this;
    }

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public OutboxMessage build() {
      return new OutboxMessage(
          aggregateType,
          aggregateId,
          eventType,
          eventVersion,
          destination,
          partitionKey,
          payload,
          headers,
          maxAttempts);
    }
  }
}
