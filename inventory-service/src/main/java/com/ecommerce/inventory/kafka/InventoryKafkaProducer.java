package com.ecommerce.inventory.kafka;

import com.ecommerce.inventory.config.KafkaTopicConfig;
import com.ecommerce.inventory.dto.InventoryFailedEvent;
import com.ecommerce.inventory.dto.InventoryReleasedEvent;
import com.ecommerce.inventory.dto.InventoryReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendInventoryReserved(InventoryReservedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_INVENTORY_RESERVED, event.orderId().toString(), event);
        log.info("Published inventory-reserved event for order {}", event.orderId());
    }

    public void sendInventoryFailed(InventoryFailedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_INVENTORY_FAILED, event.orderId().toString(), event);
        log.info("Published inventory-failed event for order {}", event.orderId());
    }

    public void sendInventoryReleased(InventoryReleasedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_INVENTORY_RELEASED, event.orderId().toString(), event);
        log.info("Published inventory-released event for order {}", event.orderId());
    }
}
