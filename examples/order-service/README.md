# Order Service Example

Minimal demo for Spring Outbox Relay:

1. Create an order
2. Persist `order.created` in the same PostgreSQL transaction
3. Relay publishes to Kafka topic `orders.events`
4. Demo consumer exposes received events at `GET /events`

## Run

```bash
docker compose up -d
cd ../..
./gradlew :examples:order-service:bootRun
```

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","total":49.90}'

curl http://localhost:8080/events
```
