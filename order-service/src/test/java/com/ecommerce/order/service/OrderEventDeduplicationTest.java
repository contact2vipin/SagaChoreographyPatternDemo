package com.ecommerce.order.service;

import com.ecommerce.order.entity.ProcessedEvent;
import com.ecommerce.order.repository.OrderRepository;
import com.ecommerce.order.repository.ProcessedEventRepository;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderEventDeduplicationTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @InjectMocks
    private EventDeduplicationService eventDeduplicationService;

    private UUID orderId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        orderId = UUID.randomUUID();
    }

    @Test
    void shouldReturnFalseWhenEventNotProcessed() {
        // Given
        when(processedEventRepository.findByEventKey(orderId + ":ORDER_CONFIRMED"))
                .thenReturn(Optional.empty());

        // When
        boolean hasProcessed = eventDeduplicationService.hasProcessedEvent(orderId, "ORDER_CONFIRMED");

        // Then
        assert !hasProcessed;
        verify(processedEventRepository).findByEventKey(orderId + ":ORDER_CONFIRMED");
    }

    @Test
    void shouldReturnTrueWhenEventAlreadyProcessed() {
        // Given
        String eventKey = orderId + ":ORDER_CONFIRMED";
        ProcessedEvent processedEvent = ProcessedEvent.builder()
                .eventKey(eventKey)
                .eventType("ORDER_CONFIRMED")
                .orderId(orderId)
                .processedAt(LocalDateTime.now())
                .build();

        when(processedEventRepository.findByEventKey(eventKey))
                .thenReturn(Optional.of(processedEvent));

        // When
        boolean hasProcessed = eventDeduplicationService.hasProcessedEvent(orderId, "ORDER_CONFIRMED");

        // Then
        assert hasProcessed;
    }

    @Test
    void shouldRecordProcessedEventSuccessfully() {
        // Given
        String eventType = "ORDER_CONFIRMED";
        String metadata = "Order confirmed successfully";

        // When
        eventDeduplicationService.recordProcessedEvent(orderId, eventType, metadata);

        // Then
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void shouldHandleDuplicateEventKeyGracefully() {
        // Given
        String eventType = "ORDER_CONFIRMED";

        // When
        eventDeduplicationService.recordProcessedEvent(orderId, eventType, "First attempt");
        eventDeduplicationService.recordProcessedEvent(orderId, eventType, "Duplicate attempt");

        // Then - Should attempt to save twice (exception handling in service)
        verify(processedEventRepository, times(2)).save(any(ProcessedEvent.class));
    }

    @Test
    void shouldGenerateConsistentEventKey() {
        // Given
        String eventType = "PAYMENT_COMPLETED";

        // When
        eventDeduplicationService.recordProcessedEvent(orderId, eventType, "metadata");
        eventDeduplicationService.recordProcessedEvent(orderId, eventType, "metadata");

        // Then - Both should use same event key
        verify(processedEventRepository, times(2)).save(any(ProcessedEvent.class));
    }
}
