# 📐 Saga Choreography Architecture Diagram

## High-Level System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│                         APACHE KAFKA MESSAGE BUS                            │
│                                                                              │
│  Topics: order-created, inventory-reserved, inventory-failed,               │
│          inventory-released, payment-completed, payment-failed,              │
│          order-confirmed, order-cancelled                                    │
│                                                                              │
└──────────────────────────┬──────────────────────────────────────────────────┘
           ▲               │                ▲                ▲
           │               │                │                │
    ┌──────┴──────┐  ┌─────┴──────┐  ┌────┴─────┐  ┌──────┴──────┐
    │   Order     │  │ Inventory  │  │ Payment  │  │ Order       │
    │   Created   │  │ Reserved   │  │Completed │  │ Confirmed   │
    │             │  │ (Success)  │  │(Success) │  │             │
    └──────┬──────┘  └─────┬──────┘  └────┬─────┘  └──────┬──────┘
           │               │              │              │
           │               │              │              │
           ▼               ▼              ▼              ▼
    ┌─────────────────────────────────────────────────────────┐
    │                                                         │
    │  SUCCESS FLOW: Order → Inventory → Payment → Confirm   │
    │                                                         │
    └─────────────────────────────────────────────────────────┘
           │               │              │              │
           │               │              │              │
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ Inventory    │  │ Payment      │  │ Order        │
    │ Failed       │  │ Failed       │  │ Cancelled    │
    │(Failure)     │  │(Compensation)│  │(Compensation)│
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           │                 │                  │
           └─────────────────┴──────────────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │  COMPENSATION FLOWS  │
           │   (Order Cancelled)  │
           └──────────────────────┘
```

---

## 🏗️ Microservices Architecture

```
┌─────────────────────────┐
│   CLIENT/REST API       │
│                         │
│  POST /orders           │
│  {customerId, amount}   │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│                    ORDER SERVICE (Port 8001)                    │
│                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐         │
│  │  Order      │  │ OrderService │  │ OrderController│         │
│  │  Entity     │  │ (Business    │  │ (REST API)     │         │
│  │             │  │  Logic +     │  │                │         │
│  │ Status:     │  │  Compensation)  │ Creates Order  │         │
│  │ PENDING     │  │                │ Publishes      │         │
│  │ CONFIRMED   │  │                │ OrderCreated   │         │
│  │ CANCELLED   │  │                │                │         │
│  └─────────────┘  └──────────────┘  └────────────────┘         │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Kafka Listeners:                                        │  │
│  │  • handleInventoryFailed() → cancelOrder()              │  │
│  │  • handlePaymentCompleted() → confirmOrder()            │  │
│  │  • handlePaymentFailed() → cancelOrder() [COMP]         │  │
│  │  • handleInventoryReleased() → cancelOrder() [COMP]     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Database: ./data/order-db (H2)                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
             │              │                  │
             │              │                  │
        PUBLISHES       CONSUMES            CONSUMES
             │              │                  │
    orderId (key)    Events from Kafka    Events from Kafka
             │              │                  │
             │              │                  │
             ▼              ▼                  ▼
           ┌────────────────────────────────────┐
           │    APACHE KAFKA MESSAGE BUS        │
           │                                    │
           │ Topics (Partitioned by orderId):   │
           │ • order-created                    │
           │ • inventory-reserved               │
           │ • inventory-failed                 │
           │ • inventory-released               │
           │ • payment-completed                │
           │ • payment-failed                   │
           │ • order-confirmed                  │
           │ • order-cancelled                  │
           │                                    │
           └────────┬───────────────┬───────────┘
                    │               │
                    ▼               ▼
        ┌──────────────────┐  ┌──────────────────┐
        │ INVENTORY SERVICE│  │ PAYMENT SERVICE  │
        │ (Port 8002)      │  │ (Port 8003)      │
        │                  │  │                  │
        │ • Reserves Stock │  │ • Processes      │
        │ • Releases Stock │  │   Payment        │
        │   (Compensation) │  │ • Simulates      │
        │                  │  │   80% success    │
        │ Listeners:       │  │                  │
        │ • Order Created  │  │ Listeners:       │
        │ • Payment Failed │  │ • Inventory      │
        │   (Comp)         │  │   Reserved       │
        │                  │  │                  │
        │ DB:              │  │ DB:              │
        │ inventory-db     │  │ payment-db       │
        └──────────────────┘  └──────────────────┘
```

---

## 📊 Event Flow Sequences

### ✅ SUCCESS FLOW

```
Time    Order Service    Inventory Service    Payment Service    Result
───────────────────────────────────────────────────────────────────────
T0      CREATE ORDER
        (PENDING)
        ↓
T1      Publish:
        OrderCreatedEvent
        ─────────────────→
T2                       CONSUME:
                         OrderCreatedEvent
                         ↓
                         Reserve Stock
                         (Check: quantity >= requested)
                         ↓ SUCCESS
                         Publish:
                         InventoryReservedEvent
                         ─────────────────────────→
T3                                              CONSUME:
                                                InventoryReservedEvent
                                                ↓
                                                Process Payment
                                                (Generate TxnID)
                                                ↓ 80% SUCCESS
                                                Publish:
                                                PaymentCompletedEvent
        ←─────────────────────────────────────────
T4      CONSUME:
        PaymentCompletedEvent
        ↓
        UPDATE ORDER:
        PENDING → CONFIRMED ✅
        ↓
        Publish:
        OrderConfirmedEvent
        
        SAGA COMPLETE ✅
        Total Time: ~100-500ms
```

### ❌ FAILURE FLOW 1: Inventory Out of Stock

```
Time    Order Service    Inventory Service    Payment Service
────────────────────────────────────────────────────────────────
T0      CREATE ORDER
        (PENDING)
        ↓
T1      Publish:
        OrderCreatedEvent
        ─────────────────→
T2                       CONSUME:
                         OrderCreatedEvent
                         ↓
                         Check: quantity < requested
                         ↓ FAIL
                         Publish:
                         InventoryFailedEvent
        ←──────────────────
T3      CONSUME:
        InventoryFailedEvent
        ↓
        UPDATE ORDER:
        PENDING → CANCELLED ❌
        (Compensation triggered)
        
        COMPENSATION: ✓ Complete
        [Payment never involved - no stock to charge]
        Total Time: ~50-200ms
```

### ❌ FAILURE FLOW 2: Payment Fails (Compensation Chain)

```
Time    Order Service    Inventory Service    Payment Service
─────────────────────────────────────────────────────────────────
T0      CREATE ORDER
        (PENDING)
        ↓
T1      Publish:
        OrderCreatedEvent
        ─────────────────→
T2                       CONSUME:
                         OrderCreatedEvent
                         ↓
                         Reserve Stock
                         ↓ SUCCESS
                         Publish:
                         InventoryReservedEvent
                         ──────────────────────→
T3                                            CONSUME:
                                              InventoryReservedEvent
                                              ↓
                                              Process Payment
                                              ↓ FAIL (20%)
                                              Publish:
                                              PaymentFailedEvent
        ←──────────────────────────────────────
T4                       CONSUME:
                         PaymentFailedEvent
                         ↓
                         COMPENSATE:
                         Release Reserved Stock
                         ↓
                         Publish:
                         InventoryReleasedEvent
        ←──────────────────
T5      CONSUME:
        InventoryReleasedEvent
        ↓
        UPDATE ORDER:
        PENDING → CANCELLED ❌
        (Compensation complete)
        
        FULL COMPENSATION: ✓ Complete
        Stock Released, Order Cancelled
        Total Time: ~200-600ms
```

---

## 🔄 Compensation Transaction Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                   SAGA COMPENSATION FLOWS                       │
└─────────────────────────────────────────────────────────────────┘

SCENARIO 1: Inventory Fails (Out of Stock)
──────────────────────────────────────────────
  Order Service                Inventory Service
  ──────────────────────────────────────────────
  
  Order: PENDING
      ↓
      └─→ OrderCreatedEvent ──→ Check Stock
                                     ↓
                                     ✗ FAIL
                                     ↓
                                 InventoryFailedEvent
      ←─────────────────────────────
      │
      └─ COMPENSATE: Cancel Order (CANCELLED) ✓


SCENARIO 2: Payment Fails (After Inventory Reserved)
─────────────────────────────────────────────────────
  Order Service   Inventory Service   Payment Service
  ───────────────────────────────────────────────────
  
  Order: PENDING
      ↓
      └─→ OrderCreatedEvent ──→ Reserve Stock ✓
                                     ↓
                                 InventoryReservedEvent
                                     ──→ Process Payment
                                             ↓
                                             ✗ FAIL (20%)
                                             ↓
                                         PaymentFailedEvent
      ←────────────────────────────────────
      │
      ├─→ Release Stock ✓ (Compensation)
      │
      └─ COMPENSATE: Cancel Order (CANCELLED) ✓


KEY PRINCIPLE:
When ANY step fails, ALL previous steps are COMPENSATED
This is achieved through event-driven choreography:

  Failed Service → Publishes FAILURE EVENT
      ↓
  Interested Services → CONSUME FAILURE EVENT
      ↓
  Trigger COMPENSATION (Reverse Operation)
      ↓
  Publish COMPENSATION EVENT
      ↓
  Continue Compensation Chain if needed
```

---

## 🗄️ Data State Transitions

### Order Entity State Machine

```
┌─────────────────────────────────────────┐
│             ORDER STATE MACHINE         │
└─────────────────────────────────────────┘

    ┌────────────────┐
    │    PENDING     │  ← Order Created
    │                │
    └────┬───────┬───┘
         │       │
    SUCCESS  FAILURE/COMPENSATION
         │       │
         ▼       ▼
    ┌──────────────────┐  ┌─────────────┐
    │   CONFIRMED      │  │  CANCELLED  │
    │   ✅ SUCCESS     │  │  ❌ FAILURE │
    └──────────────────┘  └─────────────┘


State Transitions:
─────────────────
PENDING         → CONFIRMED (PaymentCompletedEvent)
PENDING         → CANCELLED (InventoryFailedEvent)
PENDING         → CANCELLED (PaymentFailedEvent)
PENDING         → CANCELLED (InventoryReleasedEvent)

No reverse transitions (terminal states)
```

### Inventory Item State

```
┌─────────────────────────────────────────┐
│       INVENTORY ITEM LIFECYCLE          │
└─────────────────────────────────────────┘

Initial State:
  quantity: 100
  reserved: 0
  available: 100

On OrderCreatedEvent (Successful Reservation):
  quantity: 100  (unchanged)
  reserved: 1    (incremented)
  available: 99  (computed: quantity - reserved)

On PaymentFailedEvent (Compensation - Release):
  quantity: 100  (unchanged)
  reserved: 0    (decremented back)
  available: 100 (back to initial)

If Inventory Insufficient:
  available < requested
  → Do NOT reserve
  → Send InventoryFailedEvent
  → No state change
```

### Payment Entity State

```
┌─────────────────────────────────────────┐
│        PAYMENT ENTITY LIFECYCLE         │
└─────────────────────────────────────────┘

On InventoryReservedEvent:
  orderId: UUID
  amount: 100.00
  status: PENDING → (Process) → COMPLETED (80%) or FAILED (20%)
  transactionId: "TXN-..." (if success) or null (if fail)

No Compensation at Payment Level
(Compensation happens at Inventory Level)

Payment Records Are Auditable
(All attempts recorded, success/failure)
```

---

## 📡 Kafka Partitioning & Ordering

```
┌──────────────────────────────────────────────────────┐
│    KAFKA TOPIC PARTITIONING STRATEGY                │
│                                                      │
│  Key: orderId (UUID)                                │
│  Value: Event Object (JSON serialized)              │
└──────────────────────────────────────────────────────┘

Topic: order-created
├─ Partition 0: [Order-1, Order-3, Order-5, ...]
├─ Partition 1: [Order-2, Order-4, Order-6, ...]
└─ Partition 2: [Order-7, Order-8, Order-9, ...]

Topic: inventory-reserved
├─ Partition 0: [Evt(Order-1), Evt(Order-3), ...]
├─ Partition 1: [Evt(Order-2), Evt(Order-4), ...]
└─ Partition 2: [Evt(Order-7), Evt(Order-8), ...]

All events for Order-1 go to Partition 0
→ Events are processed sequentially
→ Prevents race conditions for same order
→ Enables parallelism across orders

ORDERING GUARANTEE:
  order-created (Partition 0)
     ↓
  inventory-reserved (Partition 0) [Same Key]
     ↓
  payment-completed (Partition 0) [Same Key]
     ↓
  order-confirmed (Partition 0) [Same Key]

Sequential Processing ✅ Within Same Order
Parallel Processing ✅ Across Different Orders
```

---

## 🚀 Deployment Topology

### Local Development

```
┌─────────────────────────────────────────────┐
│         Developer Machine (localhost)       │
├─────────────────────────────────────────────┤
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   Docker Container: Kafka + ZK      │   │
│  │   (docker-compose up -d)            │   │
│  │   localhost:9092                    │   │
│  └─────────────────────────────────────┘   │
│           ▲           ▲           ▲         │
│           │           │           │         │
│  ┌────────┴───┐ ┌─────┴────┐ ┌──┴────────┐ │
│  │Order Srvc  │ │Inventory │ │ Payment   │ │
│  │Port 8001   │ │Port 8002 │ │ Port 8003 │ │
│  │            │ │          │ │           │ │
│  │H2: order   │ │H2:inv    │ │H2:payment │ │
│  └────────────┘ └──────────┘ └───────────┘ │
│                                             │
│  (mvn spring-boot:run in 3 terminals)      │
│                                             │
└─────────────────────────────────────────────┘
```

### Production Deployment (Kubernetes)

```
┌──────────────────────────────────────────────────┐
│           Kubernetes Cluster                    │
├──────────────────────────────────────────────────┤
│                                                  │
│  ┌────────────────────────────────────────────┐ │
│  │     Kafka Cluster (3+ Brokers)             │ │
│  │     (Managed: Confluent Cloud, AWS MSK)    │ │
│  │     Replication Factor: 3                  │ │
│  └────────────────────────────────────────────┘ │
│                ▲           ▲           ▲        │
│   ┌────────────┴───┐ ┌─────┴────┐ ┌──┴────────┐│
│   │Order Service   │ │Inventory │ │ Payment   ││
│   │Pods (3)        │ │Pods (3)  │ │ Pods (3)  ││
│   │                │ │          │ │           ││
│   │PostgreSQL: ↙   │ │ ↙        │ │ ↙         ││
│   │order-db        │ │inventory │ │ payment   ││
│   └────────────────┘ └──────────┘ └───────────┘│
│                                                  │
│  Load Balancer (Ingress) → Port 8001 REST API  │
│                                                  │
│  Monitoring: Prometheus, Grafana, ELK Stack    │
│  Logging: Centralized (Elasticsearch)           │
│  Tracing: Jaeger (Distributed Tracing)         │
│                                                  │
└──────────────────────────────────────────────────┘
```

---

## 🎯 Architecture Benefits

```
┌─────────────────────────────────────────────────┐
│    SAGA CHOREOGRAPHY BENEFITS                  │
├─────────────────────────────────────────────────┤
│                                                 │
│  ✅ No Single Point of Failure                 │
│     (No central orchestrator)                  │
│                                                 │
│  ✅ Independent Service Scaling                │
│     (Each service can scale independently)     │
│                                                 │
│  ✅ Decoupled Communication                    │
│     (Services don't know about each other)     │
│                                                 │
│  ✅ Eventual Consistency                       │
│     (Saga ensures distributed consistency)     │
│                                                 │
│  ✅ Event Sourcing Ready                       │
│     (All events logged in Kafka)               │
│                                                 │
│  ✅ Audit Trail Built-in                       │
│     (Complete event history)                   │
│                                                 │
└─────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────┐
│    SAGA CHOREOGRAPHY CHALLENGES                │
├─────────────────────────────────────────────────┤
│                                                 │
│  ⚠️ Complex Debugging                          │
│     (State distributed across services)        │
│                                                 │
│  ⚠️ Eventual Consistency (Not Strong)          │
│     (Brief windows of inconsistency)           │
│                                                 │
│  ⚠️ Event Versioning Complexity                │
│     (Schema evolution challenges)              │
│                                                 │
│  ⚠️ Idempotency Required                       │
│     (At-least-once delivery model)             │
│                                                 │
│  ⚠️ No Built-in Timeout Handling               │
│     (Need custom saga state tracking)          │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## 📚 References

- **Microservices Pattern:** https://microservices.io/patterns/data/saga.html
- **Kafka Architecture:** https://kafka.apache.org/documentation/#design
- **Spring Kafka:** https://spring.io/projects/spring-kafka
- **Distributed Transactions:** https://www.youtube.com/watch?v=xzWhzKTD8Zg

---

**This completes the comprehensive Saga Choreography architecture documentation.**
