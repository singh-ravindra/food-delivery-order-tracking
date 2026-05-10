# Food Delivery Order Tracking

A Spring Boot + Kafka microservice demo for tracking food delivery orders through their lifecycle. This project features an embedded Kafka broker (no external dependencies required) and demonstrates event-driven architecture with multiple consumer services.

## Features
- **Order lifecycle tracking:** PLACED → ACCEPTED → PREPARING → OUT_FOR_DELIVERY → DELIVERED
- **Embedded Kafka:** Runs a Kafka broker in-process for easy local development and testing
- **Producer/Consumer pattern:** Order events are published and consumed by dedicated services
- **Sample data runner:** Automatically drives sample orders through the full lifecycle on startup
- **Structured logging:** See all state transitions and service actions in the logs

## Architecture
- **Producer:** `OrderService` — Publishes `OrderEvent` messages for each state transition
- **Consumers:**
  - `RestaurantService` — Handles order acceptance and preparation
  - `DeliveryService` — Assigns drivers and tracks delivery
  - `NotificationService` — Sends customer notifications for each state change
- **Runner:** `OrderLifecycleRunner` — Simulates multiple orders through all states on startup
- **Embedded Kafka:** Configured and started before Spring Boot for seamless integration

## Getting Started

### Prerequisites
- Java 17+
- Maven 3.8+

### Build and Run

```bash
# Build the project
mvn clean package

# Run the application (starts embedded Kafka and processes sample orders)
mvn spring-boot:run
```

Or run the JAR directly:

```bash
java -jar target/food-delivery-order-tracking-1.0-SNAPSHOT.jar
```

### What to Expect
- On startup, the app will:
  - Start an embedded Kafka broker
  - Create the `order-events` topic
  - Simulate several orders through all lifecycle states
  - Log all events and consumer actions

Check the logs for messages from the producer and all consumers.

## Configuration

Configuration is in `src/main/resources/application.yml`. Key settings:

```yaml
app:
  kafka:
    topic: order-events
    partitions: 3
    replication-factor: 1
```

## Project Structure

- `com.fooddelivery`
  - `OrderEvent`, `OrderState` — Core domain model
  - `producer.OrderService` — Publishes order events
  - `consumer.RestaurantService`, `DeliveryService`, `NotificationService` — Event consumers
  - `runner.OrderLifecycleRunner` — Drives sample orders
  - `config.*` — Embedded Kafka and Kafka client configuration

## Extending
- Add new consumers for additional business logic
- Integrate with external Kafka clusters by adjusting configuration
- Replace the runner with real order input for production scenarios

## License
MIT

---

## Code Flow

The application simulates the full lifecycle of multiple food delivery orders using Kafka events and multiple services. Here is the high-level flow:

1. **Startup**
   - The application starts an embedded Kafka broker before Spring Boot initializes.
   - The `order-events` topic is created automatically.
   - `OrderLifecycleRunner` waits for all consumers to be assigned partitions.

2. **Order Simulation**
   - `OrderLifecycleRunner` generates several random order IDs.
   - For each order:
     - Publishes a `PLACED` event via `OrderService`.
     - Advances the order through each state: `PLACED → ACCEPTED → PREPARING → OUT_FOR_DELIVERY → DELIVERED`.
     - Each state transition is published as a Kafka event.

3. **Event Consumption**
   - **RestaurantService** reacts to `PLACED`, `ACCEPTED`, and `PREPARING` events.
   - **DeliveryService** reacts to `PREPARING`, `OUT_FOR_DELIVERY`, and `DELIVERED` events.
   - **NotificationService** sends a notification for every state transition.
   - All services log their actions for observability.

4. **Completion**
   - After all orders reach `DELIVERED`, the runner logs completion and waits for user exit.

### Sequence Diagram

```
OrderLifecycleRunner
    |
    |-- placeOrder(PLACED) --> Kafka (order-events)
    |-- advance(ACCEPTED) --> Kafka
    |-- advance(PREPARING) --> Kafka
    |-- advance(OUT_FOR_DELIVERY) --> Kafka
    |-- advance(DELIVERED) --> Kafka

Kafka (order-events)
    |---> RestaurantService (PLACED, ACCEPTED, PREPARING)
    |---> DeliveryService (PREPARING, OUT_FOR_DELIVERY, DELIVERED)
    |---> NotificationService (all states)
```

- All actions and state transitions are visible in the logs.
- The flow is fully automated on startup for easy local testing and demonstration.
