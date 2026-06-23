# Saga Choreography Pattern Demo - Build & Run Instructions

## Quick Start

### 1. Start Kafka (Docker Compose)
```bash
docker-compose up -d
docker-compose logs -f kafka  # Wait until "started" message appears
```

### 2. Build All Services
```bash
mvn clean install
```

### 3. Run Services (3 separate terminals)

**Terminal 1 - Order Service:**
```bash
cd order-service
mvn spring-boot:run
# Service available at http://localhost:8001
```

**Terminal 2 - Inventory Service:**
```bash
cd inventory-service
mvn spring-boot:run
# Service available at http://localhost:8002
```

**Terminal 3 - Payment Service:**
```bash
cd payment-service
mvn spring-boot:run
# Service available at http://localhost:8003
```

---

## Testing the Event Flows

### Success Flow Test
```bash
# Create an order (80% of the time this will succeed with CONFIRMED status)
curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "totalAmount": 99.99
  }'

# Expected: Order created, events published, order becomes CONFIRMED or CANCELLED
# Watch logs to see event flow

# Get Customer orders
curl -X GET http://localhost:8001/api/v1/customers/f47ac10b-58cc-4372-a567-0e02b2c3d479

# Response:
#[
#    {
#        "orderId": "ff174692-29cc-4c34-9a99-916dda3d15b9",
#        "status": "CONFIRMED",
#        "totalAmount": 123.00,
#        "createdAt": "2026-06-23T11:28:28.532253",
#        "updatedAt": "2026-06-23T11:28:39.590662"
#    }
#    {
#        "orderId": "4b096d6a-70d7-47e5-af75-ec7b5ed9d3d5",
#        "status": "CANCELLED",
#        "totalAmount": 1234.00,
#        "createdAt": "2026-06-23T11:30:40.967004",
#        "updatedAt": "2026-06-23T11:30:45.840756"
#    }
#]

# Get Order by Id
curl -X GET http://localhost:8001/api/v1/orders/da58a893-5392-4649-af30-27ec2ada5883

# Response:
#{
#    "orderId": "da58a893-5392-4649-af30-27ec2ada5883",
#    "status": "CONFIRMED",
#    "totalAmount": 1234.00,
#    "createdAt": "2026-06-23T11:59:00.646103",
#    "updatedAt": "2026-06-23T11:59:03.465206"
#}

```
```

### Monitor Kafka Events
```bash
# In separate terminals, monitor each topic:
kafka-console-consumer.sh --topic order-created --bootstrap-server localhost:9092 --from-beginning
kafka-console-consumer.sh --topic inventory-reserved --bootstrap-server localhost:9092 --from-beginning
kafka-console-consumer.sh --topic payment-completed --bootstrap-server localhost:9092 --from-beginning
kafka-console-consumer.sh --topic payment-failed --bootstrap-server localhost:9092 --from-beginning
```

### View Databases
```bash
# Order Service H2 Console
http://localhost:8001/h2-console
# JDBC URL: jdbc:h2:file:./data/order-db

# Inventory Service H2 Console
http://localhost:8002/h2-console
# JDBC URL: jdbc:h2:file:./data/inventory-db

# Payment Service H2 Console
http://localhost:8003/h2-console
# JDBC URL: jdbc:h2:file:./data/payment-db
```

### Kafka UI
```bash
# Monitor topics and messages visually
http://localhost:8080
```

---

## Event Flow Summary

### ✅ SUCCESS FLOW (80% success rate for payment)
Order Created → Inventory Reserved → Payment Completed → Order Confirmed

### ❌ FAILURE FLOWS
1. **Inventory Fails:** Order Created → Inventory Failed → Order Cancelled
2. **Payment Fails:** Order Created → Inventory Reserved → Payment Failed → Inventory Released → Order Cancelled

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka Broker (9092)                        │
│  Topics: order-created, inventory-reserved, payment-completed, │
│          inventory-failed, payment-failed, inventory-released   │
└─────────────────────────────────────────────────────────────────┘
          ▲               ▲               ▲
          │               │               │
        ┌─┴──┐          ┌─┴──┐          ┌─┴──┐
        │    │          │    │          │    │
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ Order Service    │ │ Inventory Srvc   │ │ Payment Service  │
│ (Port 8001)      │ │ (Port 8002)      │ │ (Port 8003)      │
│                  │ │                  │ │                  │
│ Initiates Saga   │ │ Reserves Stock   │ │ Processes Charge │
│ Handles Comp.    │ │ Releases (Comp)  │ │ Simulates Fails   │
│                  │ │                  │ │                  │
│ DB: order-db     │ │ DB: inventory-db │ │ DB: payment-db   │
└──────────────────┘ └──────────────────┘ └──────────────────┘
```

---

## Troubleshooting

### Services can't connect to Kafka
- Ensure Kafka is running: `docker ps | grep kafka`
- Check bootstrap-servers: `localhost:9092`
- Verify Docker network: `docker network ls`

### H2 Databases not initializing
- Delete `./data/` directory
- Restart services (will recreate H2 files)
- Check `hibernate.ddl-auto: create-drop` in application.yml

### Payment Service always fails
- This is simulated! 20% failure rate is intentional
- Check logs for payment processing messages

### Cleanup & Reset
```bash
# Stop all services (Ctrl+C in each terminal)
# Stop Kafka
docker-compose down

# Clean databases
rm -rf ./data/

# Restart Kafka
docker-compose up -d

# Restart services
```

---

## Production Deployment Notes

Current implementation is **demo-grade**. For production:

1. ✅ Add **Transactional Outbox** pattern (DB + Kafka atomicity)
2. ✅ Implement **Idempotent consumers** (deduplication)
3. ✅ Add **Saga state table** for monitoring/recovery
4. ✅ Use **PostgreSQL** instead of H2
5. ✅ Add **circuit breakers** (Resilience4j)
6. ✅ Setup **centralized logging** (ELK stack)
7. ✅ Implement **event versioning** for schema evolution
8. ✅ Add **comprehensive error handling** with retries

See README.md for detailed architectural guidelines.
