# Initial GitHub issues for spring-outbox-relay
#
# Create with:
#   gh issue create --title "..." --body "..."
#
# Or from this checklist after the remote repository exists.

- [ ] feat: persist outbox events with Spring JDBC
- [ ] feat: claim pending events using PostgreSQL SKIP LOCKED
- [ ] feat: add Kafka publication adapter
- [ ] feat: implement exponential backoff retry policy
- [ ] feat: recover abandoned processing locks
- [ ] feat: mark permanently failed events
- [ ] feat: expose Micrometer relay metrics
- [ ] feat: add published event cleanup
- [ ] test: verify concurrent workers do not claim the same event
- [ ] test: verify business data and outbox event rollback together
- [ ] test: verify recovery after Kafka outage
- [ ] docs: explain at-least-once delivery guarantees
- [ ] docs: compare the project with Spring Modulith event publication
