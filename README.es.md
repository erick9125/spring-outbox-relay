# Spring Outbox Relay

**Publica eventos respaldados en base de datos de forma confiable desde aplicaciones Spring Boot.**

Spring Outbox Relay guarda eventos de dominio en la misma transacción de PostgreSQL que
tus cambios de negocio y los publica hacia Apache Kafka mediante un relay concurrente,
recuperable y observable.

[English](README.md) · [Español](README.es.md)

**En esta guía:** [por qué existe](#por-qué-existe-esta-librería) · [qué hace](#qué-hace) · [garantías](#garantías-de-entrega) · [instalación](#instalación) · [esquema](#esquema-de-base-de-datos) · [configuración](#configuración) · [uso](#uso) · [fallos](#comportamiento-ante-fallos-y-recuperación) · [observabilidad](#observabilidad) · [ejemplo](#aplicación-de-ejemplo)

---

## Por qué existe esta librería

Persistir un cambio en PostgreSQL y publicar un mensaje en Kafka no forman una sola
transacción distribuida. Si el commit de la base de datos tiene éxito y Kafka falla, los
consumidores nunca ven el evento y el sistema queda inconsistente.

El patrón transactional outbox resuelve esa inconsistencia escribiendo el evento en una
tabla `outbox_event` dentro de la misma transacción que los datos de negocio. La parte
difícil empieza después del commit:

- reclamar filas pendientes sin procesamiento doble
- publicar ante fallos y reinicios
- reintentar errores recuperables
- recuperar locks abandonados
- observar el backlog sin métricas ruidosas

Spring Outbox Relay se concentra en ese relay operativo: no en explicar el patrón ni en
convertirse en una plataforma completa de eventos.

| Problema | Cómo ayuda esta librería |
| --- | --- |
| Inconsistencia por dual-write | La fila de negocio y el evento outbox comparten una transacción PostgreSQL |
| Varias instancias concurrentes | `FOR UPDATE SKIP LOCKED` reclama lotes disjuntos |
| Caídas del broker | Los fallos recuperables se reprograman con backoff y jitter |
| Instancias caídas | Los locks `PROCESSING` abandonados se recuperan automáticamente |
| Mensajes irrecuperables | Los fallos permanentes o agotados quedan en estado `FAILED` inspeccionable |
| Operación en producción | Métricas Micrometer y spans para backlog y latencia de publicación |
| Control del esquema | Tabla, estados y SQL JDBC explícitos, sin adoptar Spring Modulith |

> Guarda el evento con el cambio de negocio. Publícalo en Kafka después del commit. Sobrevive a los fallos.

---

## Qué hace

1. Recibe un `OutboxMessage` dentro de un método `@Transactional` de Spring
2. Serializa el payload e inserta una fila en `outbox_event`
3. Confirma junto con tus escrituras de negocio, o hace rollback con ellas
4. Consulta eventos pendientes por lotes
5. Reclama un lote con una transacción corta de base de datos
6. Publica cada evento en Kafka
7. Marca la fila como `PUBLISHED`, la reprograma o la marca como `FAILED`

```text
Caso de uso
   │
   ▼
Transacción PostgreSQL
   ├── entidad de negocio
   └── evento outbox
           │
           ▼
         commit
           │
           ▼
El relay reclama un lote PENDING (SKIP LOCKED)
           │
           ▼
Publicación en Kafka
           │
           ├─► PUBLISHED
           ├─► PENDING   (reintento posterior)
           └─► FAILED    (agotado / permanente)
```

---

## Garantías de entrega

Spring Outbox Relay ofrece publicación **at-least-once**.

Un evento puede entregarse más de una vez si ocurre un fallo después de que Kafka acepta
el mensaje y antes de marcar la fila outbox como `PUBLISHED`. Los consumidores deben
deduplicar usando el ID estable del evento, propagado en el header Kafka `event-id`.

| Concern | Garantía |
| --- | --- |
| Transacción de base de datos | Cambio de negocio + evento outbox = atómico |
| Publicación al broker | Al menos una vez |
| Consumidor | Debe manejar IDs de evento duplicados |

Esta librería **no** promete exactly-once entre PostgreSQL y Kafka. Esa precisión es más
útil en producción que anunciar una garantía que el modelo no puede sostener.

Consulta [docs/delivery-guarantees.md](docs/delivery-guarantees.md).

---

## Requisitos

- Java 21+
- Spring Boot 3.x
- Spring JDBC
- PostgreSQL
- Apache Kafka
- Flyway (o un mecanismo equivalente para aplicar el esquema incluido)

La persistencia del núcleo usa Spring JDBC a propósito. El claim del outbox necesita SQL
preciso, actualizaciones condicionales y bloqueos de corta duración. JPA no es obligatorio.

---

## Instalación

### Maven

```xml
<dependency>
    <groupId>io.github.erick9125</groupId>
    <artifactId>spring-outbox-relay</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("io.github.erick9125:spring-outbox-relay:0.1.0")
```

La librería mantiene a propósito una huella mínima de dependencias: no arrastra ni Kafka ni
Flyway a tu classpath, así que adoptarla no puede activar una herramienta de migraciones que
no pediste. Declara tú lo que uses:

- `spring-boot-starter-jdbc`
- driver de PostgreSQL
- `spring-kafka` — solo necesario para el adaptador Kafka incluido; opcional si aportas tu
  propio `MessageBrokerPublisher`
- Flyway (recomendado, para aplicar el esquema incluido)

La librería incluye auto-configuración de Spring Boot. Cuando existen un `DataSource` y un
`KafkaTemplate`, se registran automáticamente el publisher, el relay, la recuperación de
locks y la limpieza de eventos publicados. Sin `KafkaTemplate` —y sin ningún otro bean
`MessageBrokerPublisher`— el publisher se registra igual, de modo que los eventos se
acumulan de forma durable, y el relay y su planificación quedan apagados.

---

## Esquema de base de datos

La migración se publica en `classpath:db/outbox`, deliberadamente fuera de la ubicación por
defecto de Flyway (`classpath:db/migration`): una migración de librería en la ubicación por
defecto colisionaría con el `V1__…` de tu aplicación y rompería el arranque. Añade la
ubicación junto a la tuya:

```yaml
spring:
  flyway:
    locations:
      - classpath:db/migration
      - classpath:db/outbox
```

O crea una tabla equivalente por tu cuenta:

```sql
CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(150) NOT NULL,
    event_type VARCHAR(150) NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    destination VARCHAR(200) NOT NULL,
    partition_key VARCHAR(200),
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(30) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(200),
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX idx_outbox_event_polling
    ON outbox_event (status, available_at, created_at);

CREATE INDEX idx_outbox_event_recovery
    ON outbox_event (status, locked_at)
    WHERE status = 'PROCESSING';
```

Ciclo de vida del evento:

| Estado | Significado |
| --- | --- |
| `PENDING` | Listo para reclamar cuando `available_at <= now()` |
| `PROCESSING` | Reclamado por una instancia del relay |
| `PUBLISHED` | Aceptado por Kafka y confirmado localmente |
| `FAILED` | Reintentos agotados o error de publicación permanente |

---

## Configuración

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/orders
    username: orders
    password: secret
  kafka:
    bootstrap-servers: localhost:9092
  outbox:
    relay:
      enabled: true
      batch-size: 100
      poll-interval: 1s
      lock-timeout: 5m
      default-max-attempts: 5
      instance-id: order-service-1
      retry:
        initial-delay: 5s
        maximum-delay: 5m
        multiplier: 2.0
        jitter: 0.2
      cleanup:
        enabled: true
        retention: 7d
        interval: 1h
```

| Propiedad | Valor por defecto | Descripción |
| --- | --- | --- |
| `spring.outbox.relay.enabled` | `true` | Activa la auto-configuración y los schedulers |
| `spring.outbox.relay.batch-size` | `100` | Eventos reclamados por polling |
| `spring.outbox.relay.poll-interval` | `1s` | Espera entre polls del relay |
| `spring.outbox.relay.lock-timeout` | `5m` | Edad a partir de la cual se recuperan locks `PROCESSING` |
| `spring.outbox.relay.default-max-attempts` | `5` | Presupuesto de reintentos por defecto |
| `spring.outbox.relay.instance-id` | hostname + pid | Identidad del worker en `locked_by` |
| `spring.outbox.relay.retry.initial-delay` | `5s` | Retraso base del backoff |
| `spring.outbox.relay.retry.maximum-delay` | `5m` | Tope del backoff |
| `spring.outbox.relay.cleanup.retention` | `7d` | Retención de filas `PUBLISHED` |

---

## Uso

### 1. Publicar dentro de una transacción de negocio

Inyecta `OutboxPublisher` y llámalo en el mismo método `@Transactional` que persiste tu
agregado:

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxPublisher outboxPublisher;

    public OrderService(OrderRepository orderRepository, OutboxPublisher outboxPublisher) {
        this.orderRepository = orderRepository;
        this.outboxPublisher = outboxPublisher;
    }

    @Transactional
    public Order createOrder(CreateOrderCommand command) {
        Order order = orderRepository.save(
            Order.create(command.customerId(), command.total())
        );

        outboxPublisher.publish(
            OutboxMessage.builder()
                .aggregateType("ORDER")
                .aggregateId(order.getId().toString())
                .eventType("order.created")
                .eventVersion(1)
                .destination("orders.events")
                .partitionKey(order.getId().toString())
                .payload(OrderCreatedEvent.from(order))
                .header("correlation-id", command.correlationId())
                .build()
        );

        return order;
    }
}
```

Si falla el insert del pedido, el evento outbox también hace rollback.
Si falla el insert del outbox, el pedido también hace rollback.

`publish()` exige una transacción activa y lanza `IllegalStateException` si no hay ninguna. Es
deliberado: llamado fuera de una transacción, el registro del outbox se commitearía por su
cuenta, la atomicidad para la que existe esta librería desaparecería, y nada lo diría — el
síntoma aparecería mucho después como divergencia entre la base de datos y el broker.

### 2. Contrato del mensaje

```java
OutboxMessage.builder()
    .aggregateType("ORDER")          // familia del agregado
    .aggregateId(orderId)            // identidad del agregado
    .eventType("order.created")      // nombre del evento
    .eventVersion(1)                 // versión del contrato
    .destination("orders.events")    // topic de Kafka
    .partitionKey(orderId)           // ordena eventos relacionados en una partición
    .payload(eventObject)            // serializado con Jackson a JSONB
    .headers(Map.of("correlation-id", "abc"))
    .maxAttempts(5)                  // override opcional
    .build();
```

Los payloads se serializan con Jackson. Versiona tus eventos de forma deliberada con
`eventType` y `eventVersion` para que los consumidores puedan evolucionar con seguridad.

### 3. Headers Kafka escritos por el relay

Cada registro publicado incluye:

| Header | Propósito |
| --- | --- |
| `event-id` | UUID estable del outbox para deduplicación |
| `event-type` | Nombre del evento |
| `event-version` | Versión del contrato |
| `aggregate-type` | Familia del agregado |
| `aggregate-id` | Identidad del agregado |

Los headers que agregues con `.header(...)` o `.headers(...)` se entregan junto a estos. Los
cinco nombres de arriba están reservados para los metadatos del relay: un header con el mismo
nombre se descarta con un WARN en lugar de entregarse como segundo valor, para que un
consumidor que deduplica por `event-id` pueda confiar en lo que lee.

### 4. Guía para consumidores

Los consumidores deben asumir entrega at-least-once:

```java
@KafkaListener(topics = "orders.events", groupId = "billing")
public void onOrderCreated(ConsumerRecord<String, String> record) {
    String eventId = header(record, "event-id");

    if (processedEventStore.exists(eventId)) {
        return;
    }

    billingService.handle(objectMapper.readValue(record.value(), OrderCreatedEvent.class));
    processedEventStore.save(eventId);
}
```

Usa `partitionKey` en el productor cuando los eventos relacionados del mismo agregado
deban conservar orden dentro de una partición de Kafka.

### 5. Disparar el relay manualmente

El job programado solo invoca `OutboxRelay.relayBatch()`. Puedes usar la misma API en
pruebas o herramientas operativas:

```java
RelayResult result = outboxRelay.relayBatch();
// result.claimed(), published(), rescheduled(), failed(), lockLost()
```

---

## Comportamiento ante fallos y recuperación

### Fallo recuperable de Kafka

```text
falla la publicación
  → attempts++
  → calcula available_at con backoff exponencial + jitter
  → status = PENDING
```

Suelen ser recuperables los timeouts, la indisponibilidad del broker, errores temporales
de red y el throttling.

### Fallo permanente

Destinos inválidos, problemas permanentes de serialización y otros errores no
reintentables mueven la fila a `FAILED` con `last_error` disponible para inspección.

### Caída del proceso después del claim

```text
el estado permanece PROCESSING
  → vence el lock-timeout
  → el job de recuperación devuelve la fila a PENDING
  → otro worker puede reclamarla
```

Esto preserva la durabilidad y puede producir duplicados si Kafka ya había aceptado el
mensaje. Es el comportamiento esperado bajo semántica at-least-once.

### Claim perdido

Un claim es un arriendo, no una propiedad: una instancia detenida puede descubrir que la fila
que reclamó ya fue recuperada y tomada por otra. Por eso las tres transiciones terminales
están protegidas por `locked_by`, de modo que un resultado tardío no pueda sobrescribir el de
la nueva dueña:

```text
worker-a reclama → se detiene más allá del lock-timeout
  → la recuperación devuelve la fila a PENDING
  → worker-b la reclama y la publica
  → el update de worker-a no coincide con ninguna fila y se descarta
```

Se cuenta en `outbox.events.lock.lost` y en `RelayResult.lockLost()`. Una tasa sostenida
significa que los lotes no terminan dentro de `lock-timeout`: súbelo, o baja `batch-size`.

Más detalle en [docs/failure-scenarios.md](docs/failure-scenarios.md) y
[docs/concurrency.md](docs/concurrency.md).

---

## Observabilidad

### Métricas

| Métrica | Significado |
| --- | --- |
| `outbox.events.created` | Eventos persistidos |
| `outbox.events.claimed` | Eventos reclamados por el relay |
| `outbox.events.published` | Publicaciones exitosas |
| `outbox.events.rescheduled` | Fallos recuperables |
| `outbox.events.failed` | Fallos permanentes o agotados |
| `outbox.events.lock.lost` | Claims reclamados por otra instancia a mitad de vuelo |
| `outbox.events.recovered` | Locks abandonados recuperados |
| `outbox.publication.duration` | Latencia de publicación |
| `outbox.pending.count` | Backlog actual |
| `outbox.oldest.pending.age` | Antigüedad del pending más viejo |

Las etiquetas mantienen baja cardinalidad: `destination`, `event_type`, `result`.
No se usan como tags valores de alta cardinalidad como `event_id` o mensajes de error
completos.

### Trazas / observations

Se crean spans para:

- `outbox.persist`
- `outbox.claim`
- `outbox.publish`
- `outbox.mark-published`

Se integran mediante Micrometer Observation y pueden exportarse a OpenTelemetry cuando
la aplicación esté configurada para ello.

---

## Relación con Spring Modulith

Spring Modulith ya incluye un registro de publicaciones de eventos de aplicación para
recuperar publicaciones incompletas.

Spring Outbox Relay es una alternativa enfocada para equipos que necesitan:

- una tabla outbox explícita, consultable y operable
- control directo del esquema, el polling, los reintentos y los adaptadores de broker
- claiming concurrente con `SKIP LOCKED`
- métricas de backlog y un estado fallido inspeccionable
- el patrón sin adoptar los límites de módulos de Spring Modulith

No se presenta como “mejor que Spring Modulith” sin benchmarks ni comparación en
producción. Elige la herramienta según el control operativo que necesites.

Consulta [docs/spring-modulith-comparison.md](docs/spring-modulith-comparison.md).

---

## Aplicación de ejemplo

El repositorio incluye un `order-service` mínimo que crea un pedido, escribe
`order.created` en el outbox, lo publica en Kafka y expone los eventos recibidos.

```bash
cd examples/order-service
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

---

## Compilar y probar

```bash
./gradlew check
```

Las pruebas de integración usan Testcontainers con PostgreSQL real y validan claiming,
rollback, reintentos, recuperación y workers concurrentes.

---

## Documentación

- [Garantías de entrega](docs/delivery-guarantees.md)
- [Modelo de concurrencia](docs/concurrency.md)
- [Escenarios de fallo](docs/failure-scenarios.md)
- [Comparación con Spring Modulith](docs/spring-modulith-comparison.md)

---

## Alcance actual

Spring Outbox Relay se concentra en una sola tarea bien hecha:

**Persistir eventos outbox en PostgreSQL y publicarlos de forma confiable hacia Kafka.**

Incluye:

- persistencia transaccional
- polling concurrente
- adaptador Kafka
- reintentos, recuperación y limpieza
- métricas y ganchos básicos de tracing

No incluye:

- RabbitMQ, Debezium/CDC ni otros motores de base de datos
- sagas, event sourcing ni consumidores de entrada
- dashboard web, Schema Registry, Avro/Protobuf
- entrega exactly-once entre sistemas

---

## Licencia

Apache License 2.0
