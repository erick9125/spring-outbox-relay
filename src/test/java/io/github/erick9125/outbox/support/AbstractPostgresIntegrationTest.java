package io.github.erick9125.outbox.support;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractPostgresIntegrationTest {

  @Container protected static final PostgreSQLContainer<?> POSTGRES;

  static {
    PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));
    postgres.withDatabaseName("outbox");
    postgres.withUsername("outbox");
    postgres.withPassword("outbox");
    POSTGRES = postgres;
  }
}
