# Event-Driven Order Pipeline

[![CI/CD](https://github.com/Chandu0501/event-driven-order-pipeline/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/Chandu0501/event-driven-order-pipeline/actions)
[![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)](https://openjdk.org/projects/jdk/17/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2-brightgreen?logo=spring)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-black?logo=apachekafka)](https://kafka.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A runnable reference implementation of a resilient order workflow using Kafka, PostgreSQL, Redis, and Spring Boot. It focuses on failure handling, per-order event ordering, bounded idempotency, and observable state transitions.

> **Scope:** This is a portfolio system, not a production deployment. Performance numbers are intentionally not claimed without a reproducible run. A k6 workload is included under `benchmarks/` so results can be generated and reviewed on the reader's own hardware.

---

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                        Event-Driven Order Pipeline                          │
 └─────────────────────────────────────────────────────────────────────────────┘

  REST Client
      │
      ▼
 ┌──────────────┐   POST /api/v1/orders
 │ OrderController│─────────────────────────────────────┐
 └──────────────┘                                       │
      │                                                 ▼
      │                                        ┌──────────────┐
      │                                        │ OrderService │
      │                                        │  (+ Circuit  │
      │                                        │   Breaker)   │
      │                                        └──────┬───────┘
      │                                               │  save()
      │                                               ▼
      │                                        ┌──────────────┐
      │                                        │  PostgreSQL  │
      │                                        │  (orders)    │
      │                                        └──────────────┘
      │                                               │
      │                                               │  publish event
      │                                               ▼
      │                                   ┌───────────────────────┐
      │                                   │   OrderEventProducer  │
      │                                   │  (idempotent producer │
      │                                   │   key=orderId)        │
      │                                   └───────────┬───────────┘
      │                                               │
      │                                               ▼
      │                                 ┌─────────────────────────┐
      │                                 │   Kafka Topic           │
      │                                 │   order.events.v1       │
      │                                 │   [partition 0│1│2]     │
      │                                 └────────────┬────────────┘
      │                                              │         │ on 3× failure
      │                                              │         ▼
      │                                              │  ┌──────────────┐
      │                                              │  │  .DLT topic  │
      │                                              │  │  (dead letter)│
      │                                              │  └──────────────┘
      │                                              │
      │                                              ▼
      │                                 ┌─────────────────────────┐
      │                                 │   OrderEventConsumer    │
      │                                 │   (3 concurrent threads)│
      │                                 │   manual ACK + retry    │
      │                                 └────────────┬────────────┘
      │                                              │
      │                              ┌───────────────┼───────────────┐
      │                              ▼               ▼               ▼
      │                    ORDER_CREATED   PAYMENT_VALIDATED  INVENTORY_RESERVED
      │                          │               │                   │
      │                    validatePayment  reserveInventory     shipOrder
      │
      │        ┌──────────────────────────────────────────────────────┐
      │        │                  Supporting Services                 │
      │        │                                                      │
      │        │   ┌───────────────┐      ┌──────────────────────┐   │
      │        │   │  Redis Cache  │      │  Micrometer/Prometheus│   │
      │        │   │  (Idempotency │      │  /actuator/prometheus │   │
      │        │   │   key store)  │      │  Prometheus scrape    │   │
      │        │   └───────────────┘      └──────────────────────┘   │
      │        └──────────────────────────────────────────────────────┘
```

## Order State Machine

```
  PENDING ──────────────► PAYMENT_VALIDATED ──► INVENTORY_RESERVED ──► SHIPPED ──► DELIVERED
     │                           │                      │
     │                    (circuit open)           (stock out)
     ▼                           ▼                      ▼
  FAILED ◄────────────────────────────────────────────────
     │
  (retry >= 3)
     ▼
 DEAD_LETTERED
```

---

## Key Engineering Decisions

The precise delivery guarantees and the PostgreSQL/Kafka dual-write limitation
are documented in [`docs/design-tradeoffs.md`](docs/design-tradeoffs.md).

| Concern | Solution | Why |
|---|---|---|
| **Duplicate events** | Redis `SET NX` idempotency keys (24h TTL) | Kafka at-least-once delivery can replay — guard all consumers |
| **Partition ordering** | Producer key = `orderId` | All events for one order land on same partition → sequential processing |
| **Cascading failures** | Resilience4j circuit breaker (50% threshold, 30s open) | Prevent payment service downtime from taking down the pipeline |
| **Throughput** | 32KB batch, 5ms linger, snappy compression | Tunable settings whose impact can be measured with the included k6 workload |
| **Observability** | Micrometer counters per event type + Prometheus scrape | Makes workflow failures and duplicate suppression visible |
| **Consumer safety** | Manual ACK, DLQ after 3 retries | No silent message loss on pod restart or rebalance |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.2 |
| Messaging | Apache Kafka (Confluent 7.6) |
| Database | PostgreSQL 16 + Flyway migrations |
| Cache | Redis 7.2 (idempotency store) |
| Circuit Breaker | Resilience4j 2.2 |
| Observability | Micrometer + Prometheus |
| Testing | JUnit 5 and Mockito (Testcontainers dependencies reserved for future integration tests) |
| Coverage | JaCoCo (40% whole-project minimum enforced; raise as tests expand) |
| Container | Docker + docker-compose |
| CI/CD | GitHub Actions |

---

## Running Locally

### Prerequisites
- Docker & Docker Compose
- Java 17+
- Maven 3.9+

### 1. Start infrastructure

```bash
docker-compose up -d zookeeper kafka postgres redis
```

Wait for all services to be healthy (check with `docker-compose ps`).

### 2. Run the application

```bash
mvn spring-boot:run
```

Or with Docker:

```bash
docker-compose up --build app
```

### 3. Access services

| Service | URL |
|---|---|
| API | http://localhost:8080/api/v1/orders |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| Actuator / Health | http://localhost:8080/actuator/health |
| Prometheus Metrics | http://localhost:8080/actuator/prometheus |
| Kafka UI | http://localhost:8090 |

---

## API Reference

### Create Order
```http
POST /api/v1/orders
Content-Type: application/json

{
  "customerId": "cust-123",
  "productId": "prod-456",
  "quantity": 2,
  "totalAmount": 99.99
}
```

### Get Order
```http
GET /api/v1/orders/{orderId}
```

### Trigger Payment Validation
```http
POST /api/v1/orders/{orderId}/validate-payment
```

### Get Orders by Customer
```http
GET /api/v1/orders/customer/{customerId}
```

---

## Running Tests

```bash
# Unit tests only
mvn test

# Unit tests + whole-project coverage check (40% threshold enforced)
mvn verify

# Static analysis
mvn spotbugs:check

# Reproducible API workload (requires k6 and a running stack)
k6 run benchmarks/create-orders.js
```

---

## Key Metrics

The application exposes the following custom metrics at `/actuator/prometheus`:

| Metric | Description |
|---|---|
| `orders_created_total` | Total orders ingested |
| `orders_payment_validated_total` | Successful payment validations |
| `orders_shipped_total` | Orders successfully shipped |
| `orders_dead_lettered_total` | Events exhausted retries → DLQ |
| `kafka_events_duplicate_skipped_total` | Idempotency guards triggered |
| `kafka_publish_failure_total` | Producer send failures |
| `circuit_breaker_open_total` | Circuit breaker trips |

---

## Project Structure

```
src/
├── main/java/com/chandu/orderapi/
│   ├── config/          # Kafka producer/consumer, Redis, Resilience4j config
│   ├── controller/      # REST endpoints with OpenAPI annotations
│   ├── consumer/        # Kafka listener with idempotency + manual ACK
│   ├── domain/          # Order entity, OrderEvent DTO, OrderStatus enum
│   ├── exception/       # Custom exceptions + GlobalExceptionHandler
│   ├── metrics/         # Micrometer custom counters
│   ├── producer/        # Kafka template wrapper with async callbacks
│   ├── repository/      # Spring Data JPA queries
│   └── service/         # Business logic, circuit breaker, state transitions
├── main/resources/
│   ├── db/migration/    # Flyway SQL migrations
│   └── application.yml  # Config for local + docker profiles
└── test/java/...        # Unit tests (JUnit 5 + Mockito)
```

---

## Author

**Sai Chandu Vallaboju** — Senior Software Engineer  
[GitHub](https://github.com/Chandu0501) · [LinkedIn](https://www.linkedin.com/in/saichandu-vallaboju) · [saichanduvallaboju@gmail.com](mailto:saichanduvallaboju@gmail.com)
