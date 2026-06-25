package com.ecommerce.inventory.kafka;

import com.ecommerce.inventory.dto.InventoryFailedEvent;
import com.ecommerce.inventory.dto.InventoryReleasedEvent;
import com.ecommerce.inventory.dto.InventoryReservedEvent;
import com.ecommerce.inventory.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OutboxService outboxService;

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendInventoryReserved(InventoryReservedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Inventory");
        log.debug("Inventory-reserved event published to outbox for order {}", event.orderId());
    }

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendInventoryFailed(InventoryFailedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Inventory");
        log.debug("Inventory-failed event published to outbox for order {}", event.orderId());
    }

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendInventoryReleased(InventoryReleasedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Inventory");
        log.debug("Inventory-released event published to outbox for order {}", event.orderId());
    }
}
