package com.ecommerce.payment.service;

import com.ecommerce.payment.entity.Outbox;
import com.ecommerce.payment.entity.OutboxStatus;
import com.ecommerce.payment.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxService {
    
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    
    @Transactional
    public void publishToOutbox(Object event, String aggregateId, String aggregateType) {
        try {
            String eventType = event.getClass().getSimpleName();
            String payload = objectMapper.writeValueAsString(event);
            long timestamp = System.currentTimeMillis();
            
            Outbox outbox = Outbox.builder()
                .aggregateId(aggregateId)
                .aggregateType(aggregateType)
                .eventType(eventType)
                .payload(payload)
                .timestamp(timestamp)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();
            
            outboxRepository.save(outbox);
            log.debug("Event {} published to outbox for aggregate {} ({})", 
                eventType, aggregateId, aggregateType);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize event for outbox", e);
            throw new RuntimeException("Failed to serialize event", e);
        }
    }
    
    @Transactional
    public void markAsPublished(Long outboxId) {
        Outbox outbox = outboxRepository.findById(outboxId)
            .orElseThrow(() -> new IllegalArgumentException("Outbox event not found: " + outboxId));
        
        outbox.setStatus(OutboxStatus.PUBLISHED);
        outbox.setPublishedAt(System.currentTimeMillis());
        outboxRepository.save(outbox);
        
        log.debug("Outbox event {} marked as published", outboxId);
    }
    
    @Transactional
    public void recordFailure(Long outboxId, int maxRetries) {
        Outbox outbox = outboxRepository.findById(outboxId)
            .orElseThrow(() -> new IllegalArgumentException("Outbox event not found: " + outboxId));
        
        outbox.setRetryCount(outbox.getRetryCount() + 1);
        
        if (outbox.getRetryCount() >= maxRetries) {
            outbox.setStatus(OutboxStatus.FAILED);
            log.error("Outbox event {} marked as FAILED after {} retries", 
                outboxId, outbox.getRetryCount());
        }
        
        outboxRepository.save(outbox);
    }
    
    public long countPendingEvents() {
        return outboxRepository.countPendingEvents();
    }
}
