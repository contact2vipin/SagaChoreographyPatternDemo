package com.ecommerce.inventory.kafka;

import com.ecommerce.inventory.entity.Outbox;
import com.ecommerce.inventory.entity.OutboxStatus;
import com.ecommerce.inventory.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class OutboxPoller {
    
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    @Value("${outbox.polling.batch-size:100}")
    private int batchSize;
    
    @Value("${outbox.polling.max-retries:3}")
    private int maxRetries;
    
    private final Map<String, String> eventTypeToTopicMap = new HashMap<>();
    
    public OutboxPoller(OutboxRepository outboxRepository, 
                        KafkaTemplate<String, String> kafkaTemplate,
                        ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        
        // Initialize event type to topic mapping
        this.eventTypeToTopicMap.put("OrderCreatedEvent", "order-created");
        this.eventTypeToTopicMap.put("OrderConfirmedEvent", "order-confirmed");
        this.eventTypeToTopicMap.put("OrderCancelledEvent", "order-cancelled");
        this.eventTypeToTopicMap.put("InventoryReservedEvent", "inventory-reserved");
        this.eventTypeToTopicMap.put("InventoryFailedEvent", "inventory-failed");
        this.eventTypeToTopicMap.put("InventoryReleasedEvent", "inventory-released");
        this.eventTypeToTopicMap.put("PaymentCompletedEvent", "payment-completed");
        this.eventTypeToTopicMap.put("PaymentFailedEvent", "payment-failed");
    }
    
    @Scheduled(fixedDelayString = "${outbox.polling.interval-ms:2000}")
    @Transactional
    public void pollAndPublish() {
        List<Outbox> pendingEvents = outboxRepository.findPendingEventsWithLimit(batchSize);
        
        if (pendingEvents.isEmpty()) {
            return;
        }
        
        log.debug("Polling {} pending outbox events", pendingEvents.size());
        
        for (Outbox event : pendingEvents) {
            try {
                publishToKafka(event);
                outboxRepository.flush(); // Flush to ensure published_at is persisted
            } catch (Exception e) {
                log.warn("Failed to publish outbox event {}: {}", event.getId(), e.getMessage());
                handlePublishFailure(event);
            }
        }
    }
    
    @Transactional
    private void publishToKafka(Outbox outboxEvent) throws Exception {
        String topic = eventTypeToTopicMap.getOrDefault(
            outboxEvent.getEventType(), 
            outboxEvent.getEventType().toLowerCase()
        );
        
        kafkaTemplate.send(topic, outboxEvent.getAggregateId(), outboxEvent.getPayload()).get();
        
        // Mark as published only after successful send
        outboxEvent.setStatus(OutboxStatus.PUBLISHED);
        outboxEvent.setPublishedAt(System.currentTimeMillis());
        outboxRepository.save(outboxEvent);
        
        log.debug("Outbox event {} published to topic {}", outboxEvent.getId(), topic);
    }
    
    @Transactional
    private void handlePublishFailure(Outbox event) {
        event.setRetryCount(event.getRetryCount() + 1);
        
        if (event.getRetryCount() >= maxRetries) {
            event.setStatus(OutboxStatus.FAILED);
            log.error("Outbox event {} moved to FAILED after {} retries", 
                event.getId(), event.getRetryCount());
        }
        
        outboxRepository.save(event);
    }
}
