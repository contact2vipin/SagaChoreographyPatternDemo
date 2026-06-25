package com.ecommerce.order.kafka;

import com.ecommerce.order.dto.OrderCancelledEvent;
import com.ecommerce.order.dto.OrderConfirmedEvent;
import com.ecommerce.order.dto.OrderCreatedEvent;
import com.ecommerce.order.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OutboxService outboxService;

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     * This ensures atomicity: DB write and event publish happen in same transaction
     */
    public void sendOrderCreated(OrderCreatedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Order");
        log.debug("Order-created event published to outbox for order {}", event.orderId());
    }

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendOrderConfirmed(OrderConfirmedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Order");
        log.debug("Order-confirmed event published to outbox for order {}", event.orderId());
    }

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendOrderCancelled(OrderCancelledEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Order");
        log.debug("Order-cancelled event published to outbox for order {}", event.orderId());
    }
}
