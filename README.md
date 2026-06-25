# Saga Pattern Choreography with Apache Kafka - E-Commerce Order Processing

## 📋 Project Overview

This is a **production-ready demonstration** of the **Saga Pattern using Choreography approach** with Apache Kafka in a distributed microservices architecture. It implements an e-commerce order processing system where three independent services coordinate through events without a central orchestrator.

### Architecture Highlights
- **✅ Event-Driven Choreography**: No central orchestrator; services communicate via Kafka events
- **✅ Distributed Databases**: Each service has its own isolated H2 database
- **✅ Compensating Transactions**: Failures automatically trigger rollback events
- **✅ Java Records**: Immutable event DTOs for type safety
- **✅ Spring Boot 3.3.0**: Latest Spring Boot with Spring Kafka integration

---

## 🏗️ Services Architecture

### 1. **Order Service** (Port 8001)
**Saga Initiator** - Creates orders and orchestrates compensation

**Responsibilities:**
- Creates orders in PENDING state
- Listens for inventory and payment events
- Confirms orders on successful payment
- Cancels orders on any failure

**Producers:**
- `order-created` → Initiates the saga
- `order-confirmed` → Final success state
- `order-cancelled` → Compensation triggered

**Consumers:**
- `inventory-failed` → Failure flow
- `payment-completed` → Success flow
- `payment-failed` → Compensation flow
- `inventory-released` → Final compensation step

**Database:** `./data/order-db` (H2)

---

### 2. **Inventory Service** (Port 8002)
**Stock Reservation Handler** - Reserves and releases inventory

**Responsibilities:**
- Reserves stock when order is created
- Releases stock when payment fails (compensation)

**Producers:**
- `inventory-reserved` → Success (continues saga)
- `inventory-failed` → Failure (triggers compensation)
- `inventory-released` → Compensation (stock release)

**Consumers:**
- `order-created` → Attempt stock reservation
- `payment-failed` → Release reserved stock (compensation)

**Database:** `./data/inventory-db` (H2)

---

### 3. **Payment Service** (Port 8003)
**Payment Processor** - Charges customer and handles failures

**Responsibilities:**
- Processes payment after inventory is reserved
- Simulates 80% success / 20% failure rate (for demo)

**Producers:**
- `payment-completed` → Success (confirms order)
- `payment-failed` → Failure (triggers compensation)

**Consumers:**
- `inventory-reserved` → Attempt payment charge

**Database:** `./data/payment-db` (H2)

---

## 🔄 Event Flows

### ✅ SUCCESS FLOW (Happy Path)

```
1. Order Service
   └─ POST /orders {customerId, totalAmount}
      └─ Creates order (PENDING)
      └─ Publishes: OrderCreatedEvent → order-created

2. Inventory Service
   └─ Consumes: OrderCreatedEvent
   └─ Reserves stock
   └─ Publishes: InventoryReservedEvent → inventory-reserved

3. Payment Service
   └─ Consumes: InventoryReservedEvent
   └─ Processes payment (80% success)
   └─ Publishes: PaymentCompletedEvent → payment-completed

4. Order Service
   └─ Consumes: PaymentCompletedEvent
   └─ Updates order (CONFIRMED)
   └─ Publishes: OrderConfirmedEvent → order-confirmed
```

**Final State:** Order = CONFIRMED ✅

---

### ❌ FAILURE FLOW 1: Inventory Out of Stock

```
1. Order Service
   └─ Creates order (PENDING)
   └─ Publishes: OrderCreatedEvent

2. Inventory Service
   └─ Attempts stock reservation
   └─ Fails (insufficient stock)
   └─ Publishes: InventoryFailedEvent → inventory-failed

3. Order Service (COMPENSATION)
   └─ Consumes: InventoryFailedEvent
   └─ Updates order (CANCELLED)
   └─ Publishes: OrderCancelledEvent → order-cancelled

[Payment Service never triggered - only listens to inventory-reserved]
```

**Final State:** Order = CANCELLED 🚫

---

### ❌ FAILURE FLOW 2: Payment Fails (Compensation Chain)

```
1. Order Service
   └─ Creates order (PENDING)
   └─ Publishes: OrderCreatedEvent

2. Inventory Service
   └─ Reserves stock successfully
   └─ Publishes: InventoryReservedEvent

3. Payment Service
   └─ Consumes: InventoryReservedEvent
   └─ Payment fails (simulated 20% failure)
   └─ Publishes: PaymentFailedEvent → payment-failed

4. Inventory Service (COMPENSATION)
   └─ Consumes: PaymentFailedEvent
   └─ Releases reserved stock
   └─ Publishes: InventoryReleasedEvent → inventory-released

5. Order Service (COMPENSATION)
   └─ Consumes: InventoryReleasedEvent
   └─ Updates order (CANCELLED)
   └─ Publishes: OrderCancelledEvent → order-cancelled
```

**Final State:** Order = CANCELLED, Stock = Released 🔄

---

## 🚀 Getting Started

### Prerequisites
- **Java 17+**
- **Maven 3.8+**
- **Apache Kafka 3.x** (running on localhost:9092)
- **H2 Database** (embedded in services)

### Install & Run Kafka

**Option 1: Docker Compose** (Recommended)
```bash
# Create docker-compose.yml in project root
docker-compose up -d

# Verify Kafka is running
docker exec -it <kafka-container> kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Option 2: Local Kafka Installation**
```bash
# Download from https://kafka.apache.org/
# https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
tar -xzf kafka_2.13-3.9.1.tgz
rename kafka_2.13-3.9.1 to Kafka
D:\> cd Kafka

# Start Zookeeper
D:\Kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Start Kafka (new terminal)
D:\Kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties
```

### Build & Run Services

```bash
# Navigate to project root
cd SagaChoreographyPatternDemo

# Build all services
mvn clean install

# Terminal 1: Start Order Service
cd order-service
mvn spring-boot:run

# Terminal 2: Start Inventory Service
cd inventory-service
mvn spring-boot:run

# Terminal 3: Start Payment Service
cd payment-service
mvn spring-boot:run
```

**Expected Output:**
```
Order Service started on http://localhost:8001
Inventory Service started on http://localhost:8002
Payment Service started on http://localhost:8003
```

---

## 🧪 Testing the Saga

### Test 1: Create an Order (Success Case)

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "totalAmount": 99.99
  }'
```

**Expected Response:**
```json
{
  "id": 1,
  "orderId": "a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4a5b6c",
  "customerId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "PENDING",
  "totalAmount": 99.99,
  "createdAt": "2024-06-22T15:30:00",
  "updatedAt": "2024-06-22T15:30:00"
}
```

**Monitor Event Flow:**

Open three terminals to see service logs:

```bash
# Terminal 4 - Monitor events
watch -n 1 "curl -s http://localhost:8001/orders | head -20"
```

**Expected Event Chain:**
1. ✅ Order created (PENDING)
2. ✅ OrderCreatedEvent published
3. ✅ InventoryReservedEvent published
4. ✅ PaymentCompleted/Failed event published
5. ✅ Order updated (CONFIRMED or CANCELLED)

---

### Test 2: Simulate Failure (Monitor Logs)

Watch the service logs in real-time:

```bash
# Terminal for Order Service logs
tail -f order-service.log

# In another terminal, create order
curl -X POST http://localhost:8001/orders ...
```

**Observe:**
- 80% of requests → Order = CONFIRMED ✅
- 20% of requests → Order = CANCELLED (payment failure) 🔄

---

## 📊 Database Inspection

### View Order Service Database
```bash
# Access H2 Console
http://localhost:8001/h2-console

# JDBC URL: jdbc:h2:file:./data/order-db
# Username: sa
# Password: (leave empty)

# Query orders
SELECT * FROM orders;
```

### View Inventory Service Database
```bash
# JDBC URL: jdbc:h2:file:./data/inventory-db
# Query inventory
SELECT product_id, quantity, reserved, quantity - reserved AS available FROM inventory_items;
```

### View Payment Service Database
```bash
# JDBC URL: jdbc:h2:file:./data/payment-db
# Query payments
SELECT * FROM payments;
```

---

## 🔍 Kafka Topic Inspection

### List All Topics
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Expected Topics:**
- `order-created`
- `inventory-reserved`
- `inventory-failed`
- `inventory-released`
- `payment-completed`
- `payment-failed`
- `order-confirmed`
- `order-cancelled`

### Monitor Topic Messages
```bash
# Watch order-created events
kafka-console-consumer.sh --topic order-created --bootstrap-server localhost:9092 --from-beginning

# Watch inventory-reserved events
kafka-console-consumer.sh --topic inventory-reserved --bootstrap-server localhost:9092 --from-beginning

# Watch payment-completed events
kafka-console-consumer.sh --topic payment-completed --bootstrap-server localhost:9092 --from-beginning
```

---

## 📁 Project Structure

```
SagaChoreographyPatternDemo/
├── pom.xml (Parent POM)
│
├── order-service/
│   ├── pom.xml
│   └── src/main/java/com/ecommerce/order/
│       ├── OrderServiceApplication.java
│       ├── entity/
│       │   ├── Order.java
│       │   └── OrderStatus.java (enum)
│       ├── dto/ (Event Records)
│       │   ├── OrderCreatedEvent.java
│       │   ├── InventoryFailedEvent.java
│       │   ├── PaymentCompletedEvent.java
│       │   ├── PaymentFailedEvent.java
│       │   ├── InventoryReleasedEvent.java
│       │   ├── OrderConfirmedEvent.java
│       │   └── OrderCancelledEvent.java
│       ├── repository/
│       │   └── OrderRepository.java
│       ├── service/
│       │   └── OrderService.java
│       ├── kafka/
│       │   ├── OrderKafkaProducer.java
│       │   └── OrderKafkaListener.java (COMPENSATING TRANSACTIONS)
│       ├── config/
│       │   ├── KafkaTopicConfig.java
│       │   ├── KafkaProducerConfig.java
│       │   └── KafkaConsumerConfig.java
│       └── controller/
│           └── OrderController.java
│   └── src/main/resources/
│       └── application.yml
│
├── inventory-service/
│   ├── pom.xml
│   └── src/main/java/com/ecommerce/inventory/
│       ├── InventoryServiceApplication.java
│       ├── entity/
│       │   └── InventoryItem.java
│       ├── dto/
│       │   ├── OrderCreatedEvent.java
│       │   ├── PaymentFailedEvent.java
│       │   ├── InventoryReservedEvent.java
│       │   ├── InventoryFailedEvent.java
│       │   └── InventoryReleasedEvent.java
│       ├── repository/
│       │   └── InventoryRepository.java
│       ├── service/
│       │   └── InventoryService.java (COMPENSATING TRANSACTIONS)
│       ├── kafka/
│       │   ├── InventoryKafkaProducer.java
│       │   └── InventoryKafkaListener.java
│       ├── config/
│       │   ├── KafkaTopicConfig.java
│       │   ├── KafkaProducerConfig.java
│       │   └── KafkaConsumerConfig.java
│   └── src/main/resources/
│       └── application.yml
│
└── payment-service/
    ├── pom.xml
    └── src/main/java/com/ecommerce/payment/
        ├── PaymentServiceApplication.java
        ├── entity/
        │   ├── Payment.java
        │   └── PaymentStatus.java (enum)
        ├── dto/
        │   ├── InventoryReservedEvent.java
        │   ├── PaymentCompletedEvent.java
        │   └── PaymentFailedEvent.java
        ├── repository/
        │   └── PaymentRepository.java
        ├── service/
        │   └── PaymentService.java
        ├── kafka/
        │   ├── PaymentKafkaProducer.java
        │   └── PaymentKafkaListener.java
        ├── config/
        │   ├── KafkaTopicConfig.java
        │   ├── KafkaProducerConfig.java
        │   └── KafkaConsumerConfig.java
        └── src/main/resources/
            └── application.yml
```

---

## 🎯 Key Technical Decisions

### 1. **Java Records for Events**
- ✅ Immutable DTOs perfect for event data
- ✅ Type-safe and concise
- ✅ No boilerplate (no getters/setters)

### 2. **Separate Databases per Service**
- ✅ True microservices isolation
- ✅ Independent deployment
- ✅ Scalable per-service resources

### 3. **Choreography vs Orchestration**
- ✅ No single point of failure (no orchestrator)
- ⚠️ Harder to debug (state is distributed)
- ⚠️ Requires strong event design

### 4. **H2 Database**
- ✅ Zero configuration (embedded)
- ✅ Perfect for demos/testing
- ✅ Easy switch to PostgreSQL in production

### 5. **Event Partitioning**
- ✅ Kafka topics partitioned by `orderId`
- ✅ Ensures event ordering per order
- ✅ Enables horizontal scaling

---

## 🔧 Production Considerations

### ✅ Implemented: Idempotency Guarantees

This implementation now supports **at-least-once delivery semantics** with event deduplication:

**How It Works:**
1. **Event Deduplication Layer**
   - Each service maintains a `processed_events` table to track handled events
   - Event key format: `orderId:eventType` (unique constraint enforced)
   - Before processing any event, check if already handled

2. **Idempotent Listeners**
   - `OrderKafkaListener`: Deduplicates INVENTORY_FAILED, PAYMENT_COMPLETED, PAYMENT_FAILED events
   - `InventoryKafkaListener`: Deduplicates ORDER_CREATED, PAYMENT_FAILED events
   - `PaymentKafkaListener`: Deduplicates INVENTORY_RESERVED events

3. **Idempotent Business Logic**
   - **Payment Service**: Cached result prevents double-charging same order
   - **Inventory Service**: Uses `released` flag to prevent over-release
   - **Order Service**: Checks existing status before state transitions (already idempotent)

**Architecture:**
- `EventDeduplicationService`: Core deduplication logic (query + record processed events)
- `ProcessedEventRepository`: DB access for event tracking
- Transactional operations ensure atomic recording of processed events

**Testing Duplicate Delivery:**
```bash
# Manually publish same event twice to a topic to verify deduplication
kafka-console-producer --topic order-created --bootstrap-server localhost:9092
# Type the same JSON payload twice
# Second delivery will be skipped (logged as duplicate)
```

---

### ✅ Implemented: Transactional Outbox Pattern

This implementation now uses the **Transactional Outbox Pattern** to ensure atomic writes to database AND Kafka publishing:

#### What Problem Does It Solve?

**Scenario 1: DB Write Succeeds, Kafka Publish Fails**
```
Order created in DB ✓
OrderCreatedEvent published to Kafka ✗ (network fails)
→ Event is lost! Saga never proceeds. Order stuck in PENDING state.
```

**Scenario 2: Kafka Publish Succeeds, DB Write Fails**
```
OrderCreatedEvent published to Kafka ✓
Order saved to DB ✗ (constraint violation)
→ Orphaned event! Order doesn't exist but saga is processing.
```

#### How It Works (Hybrid Strategy)

**Phase 1: Atomic Outbox Write**
- Events written to `OUTBOX` table in same transaction as business logic
- Either both succeed (atomically) or both fail (no inconsistency)
- Outbox table persists events even if Kafka is temporarily down

**Phase 2A: Polling-Based Publishing (Default)**
- `OutboxPoller` scheduled task runs every 2 seconds
- Scans `OUTBOX` table for unpublished events
- Publishes to Kafka with retry logic
- Marks events as `PUBLISHED` after successful send
- **Low latency**: <5 seconds from DB write to Kafka publish

**Phase 2B: CDC-Based Publishing (Optional)**
- Change Data Capture alternative for real-time publishing
- Uses Kafka Streams to monitor outbox table changes
- Lower latency than polling (milliseconds)
- Can be enabled via `outbox.cdc.enabled: true`

#### Architecture

**Outbox Table Schema (All Services):**
```sql
CREATE TABLE outbox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,              -- Order ID
    aggregate_type VARCHAR(100) NOT NULL,            -- "Order", "Inventory", "Payment"
    event_type VARCHAR(255) NOT NULL,                -- Event class name
    payload LONGTEXT NOT NULL,                       -- Serialized JSON event
    timestamp BIGINT NOT NULL,                       -- Creation timestamp
    published_at BIGINT,                             -- NULL if not published
    retry_count INT NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',  -- PENDING, PUBLISHED, FAILED
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_outbox_agg_type_event (aggregate_id, aggregate_type, event_type),
    INDEX idx_outbox_published (published_at),
    INDEX idx_outbox_created (created_at),
    INDEX idx_outbox_status (status)
);
```

**New Components Per Service:**
- `Outbox.java`: JPA entity mapping to outbox table
- `OutboxStatus.java`: Enum for PENDING, PUBLISHED, FAILED states
- `OutboxRepository.java`: Spring Data JPA repository
- `OutboxService.java`: Business logic for atomic outbox writes
- `OutboxPoller.java`: Scheduled task for polling and publishing
- `OutboxConfig.java`: Configuration for scheduling

#### Implementation Details

**Step 1: Atomic Write to Outbox**
```java
@Transactional
public void publishToOutbox(Object event, String aggregateId, String aggregateType) {
    // Serialize event to JSON
    String payload = objectMapper.writeValueAsString(event);
    
    // Create outbox record
    Outbox outbox = Outbox.builder()
        .aggregateId(aggregateId)
        .aggregateType(aggregateType)
        .eventType(event.getClass().getSimpleName())
        .payload(payload)
        .timestamp(System.currentTimeMillis())
        .status(OutboxStatus.PENDING)
        .retryCount(0)
        .build();
    
    outboxRepository.save(outbox);  // Saves in same transaction as business logic
}
```

**Step 2: Polling & Publishing**
```java
@Scheduled(fixedDelayString = "${outbox.polling.interval-ms:2000}")
public void pollAndPublish() {
    // Get unpublished events (batch of 100)
    List<Outbox> events = outboxRepository.findPendingEventsWithLimit(100);
    
    for (Outbox event : events) {
        try {
            // Publish to Kafka with partition key = aggregateId
            kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload()).get();
            
            // Mark as published ONLY after Kafka confirms
            event.setStatus(OutboxStatus.PUBLISHED);
            event.setPublishedAt(System.currentTimeMillis());
            outboxRepository.save(event);
            
        } catch (Exception e) {
            // Increment retry count, mark as FAILED if max retries exceeded
            handlePublishFailure(event);
        }
    }
}
```

**Step 3: Existing Services Use Outbox**
```java
// Before: Direct Kafka publish (not atomic)
// orderKafkaProducer.sendOrderCreated(event);

// After: Atomic outbox write
orderKafkaProducer.sendOrderCreated(event);  // Delegates to outboxService
                                              // ↓
outboxService.publishToOutbox(event, orderId, "Order");  // Same transaction!
```

#### Configuration

**application.yml (All Services):**
```yaml
outbox:
  polling:
    enabled: true                  # Enable polling-based publisher
    interval-ms: 2000             # Poll every 2 seconds
    batch-size: 100               # Max events per poll
    max-retries: 3                # Max retries before marking FAILED
    retry-backoff-ms: 1000        # Backoff time between retries
  cdc:
    enabled: false                # Optional: enable CDC-based publisher
    enabled-if-polling-fails: true
```

#### Testing Transactional Outbox

**Test 1: Verify Atomicity (DB write succeeds, event reaches outbox)**
```bash
# Create an order
curl -X POST http://localhost:8001/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "f47ac10b-58cc-4372-a567-0e02b2c3d479", "totalAmount": 99.99}'

# Check H2 console: http://localhost:8001/h2-console
SELECT * FROM outbox WHERE status = 'PENDING' LIMIT 5;

# Wait 2-5 seconds, outbox should change to PUBLISHED
SELECT * FROM outbox WHERE status = 'PUBLISHED' LIMIT 5;
```

**Test 2: Verify Recovery After Kafka Outage**
```bash
# Start order service
cd order-service && mvn spring-boot:run

# Create multiple orders (without Kafka running)
# Orders persist to outbox table
for i in {1..5}; do
  curl -X POST http://localhost:8001/api/v1/orders \
    -H "Content-Type: application/json" \
    -d "{\"customerId\": \"$(uuidgen)\", \"totalAmount\": $((99 + i))}"
done

# Start Kafka
docker-compose up kafka

# Wait 2-5 seconds, OutboxPoller picks up events and publishes
# Verify via H2 or Kafka topic consumer
kafka-console-consumer --topic order-created --bootstrap-server localhost:9092 --from-beginning
```

**Test 3: Verify Retry Logic**
```bash
# Manually trigger a publish failure by stopping Kafka
# Check outbox table: retry_count should increment

SELECT id, status, retry_count FROM outbox WHERE aggregate_type = 'Order';

# After 3 retries, status should change to FAILED
SELECT id, status FROM outbox WHERE status = 'FAILED';
```

#### Industry Standards Met

✅ **Atomicity**: DB write + outbox record in single transaction (ACID-compliant)
✅ **Durability**: Outbox table persists even if Kafka is down
✅ **Idempotency**: Works seamlessly with existing ProcessedEvent deduplication
✅ **Ordering**: Kafka partition key (aggregateId) ensures per-order event ordering
✅ **Failure Recovery**: Polling ensures events aren't lost during Kafka outages
✅ **Low Latency**: Configurable polling interval (default 2 seconds)
✅ **Scalability**: Ready for distributed polling with row-level locks (optional)
✅ **Observability**: Detailed logging of poll/publish/failure events

#### Comparison: Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| **Event Durability** | Lost if Kafka down | Persisted in OUTBOX table |
| **Atomicity** | Non-atomic (2 operations) | Atomic (1 transaction) |
| **Publish Latency** | Immediate (best case) | <5 seconds (polling interval) |
| **Failure Recovery** | Manual re-publishing | Automatic (poller retries) |
| **Max Retries** | None built-in | Configurable (default 3) |
| **State Tracking** | Not tracked | Visible in outbox table (PENDING/PUBLISHED/FAILED) |

---

### Not Implemented (But Recommended for Production)

1. **Timeout Handling**
   - Pure choreography has no built-in timeouts
   - Consider saga tables to track in-flight sagas

2. **Event Versioning**
   - No backward compatibility for event schema changes
   - Add version field to events for evolution

3. **Circuit Breakers**
   - Network failures can cascade across services
   - Add Resilience4j or similar

4. **Centralized Logging**
   - Use ELK Stack (Elasticsearch, Logstash, Kibana) for correlation
   - Trace events using `orderId` as correlation ID

---

## 📝 Comments & Annotations

All listener methods include clear comments explaining:
- ✅ Which event is CONSUMED
- ✅ Which event is PRODUCED
- ✅ Part of which flow (success/failure/compensation)

All compensation methods are marked with:
```java
// COMPENSATING TRANSACTION: [Description]
```

---

## 🧹 Cleanup

### Stop Services
```bash
# Ctrl+C in each terminal
```

### Clean Databases
```bash
rm -rf ./data/
```

### Stop Kafka
```bash
docker-compose down
# or
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
```

---

## 📚 References

- **Saga Pattern:** https://microservices.io/patterns/data/saga.html
- **Apache Kafka:** https://kafka.apache.org/
- **Spring Kafka:** https://spring.io/projects/spring-kafka
- **Event Sourcing:** https://microservices.io/patterns/data/event-sourcing.html

---

## 📄 License

MIT License - Feel free to use for learning and demonstrations

---

## 👨‍💻 Author Notes

This implementation demonstrates **production-grade patterns** suitable for:
- ✅ Microservices training
- ✅ Architecture proof-of-concepts
- ✅ Design pattern demonstrations
- ✅ Learning Kafka choreography

**Not recommended for** direct production use without:
- Adding idempotency guarantees
- Implementing transactional outbox
- Adding comprehensive error handling
- Setting up centralized monitoring/logging
