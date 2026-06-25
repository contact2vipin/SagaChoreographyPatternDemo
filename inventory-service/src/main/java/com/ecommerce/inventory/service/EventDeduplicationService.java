package com.ecommerce.inventory.service;

import com.ecommerce.inventory.entity.ProcessedEvent;
import com.ecommerce.inventory.repository.ProcessedEventRepository;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventDeduplicationService {

    private final ProcessedEventRepository processedEventRepository;

    @Transactional(readOnly = true)
    public boolean hasProcessedEvent(UUID orderId, String eventType) {
        String eventKey = generateEventKey(orderId, eventType);
        boolean exists = processedEventRepository.findByEventKey(eventKey).isPresent();
        if (exists) {
            log.debug("Event already processed: {} for order {}", eventType, orderId);
        }
        return exists;
    }

    @Transactional
    public void recordProcessedEvent(UUID orderId, String eventType, String metadata) {
        String eventKey = generateEventKey(orderId, eventType);
        try {
            ProcessedEvent processedEvent = ProcessedEvent.builder()
                    .eventKey(eventKey)
                    .eventType(eventType)
                    .orderId(orderId)
                    .processedAt(LocalDateTime.now())
                    .metadata(metadata)
                    .build();
            processedEventRepository.save(processedEvent);
            log.debug("Recorded processed event: {} for order {}", eventType, orderId);
        } catch (Exception e) {
            log.warn("Failed to record processed event (may be duplicate): {} for order {}", eventType, orderId, e);
        }
    }

    private String generateEventKey(UUID orderId, String eventType) {
        return orderId + ":" + eventType;
    }
}
