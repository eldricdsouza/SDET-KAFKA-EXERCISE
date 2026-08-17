# SDET Kafka Exercise

This project is a small distributed Spring Boot application designed as the system-under-test for an SDET coding exercise. It demonstrates an asynchronous order/payment workflow with Kafka messaging, H2 persistence, and two independent services.

## 1. System overview

The application contains two services:

- Order Service: exposes REST APIs for creating and reading orders. It persists orders in H2 and publishes order-created events to Kafka.
- Payment Service: subscribes to `orders.created`, validates a deterministic payment outcome, and publishes either `payments.completed` or `payments.failed`.

The basic workflow is:

1. A client calls `POST /orders` on the Order Service.
2. The Order Service stores the order with status `PENDING`.
3. The Order Service emits an `OrderCreated` event to Kafka on the `orders.created` topic.
4. The Payment Service consumes that event.
5. If the customerId is `FAIL-PAYMENT`, the payment service emits a deterministic `PaymentFailed` event to `payments.failed`.
6. Otherwise, it emits a `PaymentCompleted` event to `payments.completed`.
7. The Order Service consumes those payment events and transitions the order to `PAID` or `PAYMENT_FAILED`.

This creates asynchronous business flow and eventual consistency that a candidate can test with both REST and Kafka-oriented assertions.

## 2. How to run

### Required tools

- Java 21
- Maven 3.9+
- Docker Desktop or Docker Engine

### Start Kafka

From the project root:

```bash
docker compose up -d
```

This starts Kafka on `localhost:9092` using a modern broker configuration without Zookeeper.

### Build the project

```bash
mvn clean package
```

### Start the services

In separate terminals:

```bash
cd order-service
mvn spring-boot:run
```

```bash
cd payment-service
mvn spring-boot:run
```

Alternatively, after `mvn clean package`, run the generated executable JARs from the project root:

```bash
java -jar order-service/target/order-service-1.0.0-SNAPSHOT.jar
```

```bash
java -jar payment-service/target/payment-service-1.0.0-SNAPSHOT.jar
```

The services use H2 in-memory databases and will run on:

- Order Service: http://localhost:8081
- Payment Service: http://localhost:8082

## 3. API examples

### Create an order

Request:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "amount": 100.50,
    "currency": "EUR"
  }'
```

Example successful response:

```json
{
  "orderId": "b3b7d8ca-1ea0-4d5c-8b6a-02d1670f9f8e",
  "customerId": "CUST-001",
  "amount": 100.50,
  "currency": "EUR",
  "status": "PENDING"
}
```

HTTP status: `201 Created`

### Get an order

```bash
curl http://localhost:8081/orders/<orderId>
```

Example response:

```json
{
  "orderId": "b3b7d8ca-1ea0-4d5c-8b6a-02d1670f9f8e",
  "customerId": "CUST-001",
  "amount": 100.50,
  "currency": "EUR",
  "status": "PAID"
}
```

HTTP status: `200 OK`

### Invalid request example

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "",
    "amount": 0,
    "currency": "USD"
  }'
```

This returns HTTP `400 Bad Request` with a validation error payload.

## 4. Kafka topics

The system employs the following topics:

- `orders.created`
- `payments.completed`
- `payments.failed`

### `orders.created`

Used by the Order Service to signal that a new order was accepted:

```json
{
  "eventId": "b9a6be18-93d0-4b63-ae56-09dc586f36b2",
  "eventType": "OrderCreated",
  "orderId": "b3b7d8ca-1ea0-4d5c-8b6a-02d1670f9f8e",
  "customerId": "CUST-001",
  "amount": 100.50,
  "currency": "EUR"
}
```

Message key: orderId

### `payments.completed`

Used by the Payment Service for successful payment processing:

```json
{
  "eventId": "0dc8d400-bd42-4e1f-9f73-584de33b3d0f",
  "eventType": "PaymentCompleted",
  "orderId": "b3b7d8ca-1ea0-4d5c-8b6a-02d1670f9f8e",
  "amount": 100.50,
  "currency": "EUR"
}
```

Message key: orderId

### `payments.failed`

Used by the Payment Service for deterministic payment failures:

```json
{
  "eventId": "f2cb03db-cf7f-4f62-9df2-fd4702d13d1d",
  "eventType": "PaymentFailed",
  "orderId": "b3b7d8ca-1ea0-4d5c-8b6a-02d1670f9f8e",
  "reason": "Payment declined"
}
```

Message key: orderId

## 5. Exercise description

### SDET Coding Exercise

Your task is to design and implement an automated test suite for this distributed application.

Your tests should demonstrate your ability to test:

- REST APIs
- Kafka events
- Asynchronous workflows
- End-to-end business flows
- Validation and error handling
- Payment failures
- Duplicate Kafka messages/idempotency
- Eventual consistency
- Data/state verification

You may use the testing tools and frameworks you are most comfortable with.

Your solution should be maintainable, readable, and suitable as a foundation for a production test automation framework.

Please include a README explaining:

- Your testing strategy
- Why you selected your tools/frameworks
- How you handle asynchronous Kafka processing
- How you avoid flaky tests
- What additional tests you would add in a production environment
- How you would integrate the tests into CI/CD

### Intentional testing challenges

This is deliberately an asynchronous, stateful workflow. Events may be delivered more than once, and an order's final state is eventually consistent rather than immediately available at the time of its creation. Explore the APIs, topics, and persistence behaviour to decide which scenarios and assertions your automated suite should cover.

## Notes for the candidate

- The system is intentionally simple, but the workflow includes real asynchronous messaging behaviour and deterministic edge cases.
- Kafka topics are intentionally plain and easy to inspect with tools such as Kafka console consumer or custom Kafka clients in tests.
- Payment failure is deterministic for `customerId = FAIL-PAYMENT`.
- Duplicate events are handled in a simple yet testable way using a processed-event table in the Payment Service.
- The application keeps H2 state local to each service so candidates can validate persistence through REST endpoints and direct database inspection if needed.

## Minimal smoke tests included

This repository includes only a minimal smoke test in each service to confirm the application context starts and the basic persistence layer works. The candidate is expected to create their own automation tests against the running application.
