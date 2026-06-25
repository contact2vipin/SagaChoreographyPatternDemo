package com.ecommerce.order.service;

import com.ecommerce.order.dto.OrderCreatedEvent;
import com.ecommerce.order.entity.Outbox;
import com.ecommerce.order.entity.OutboxStatus;
import com.ecommerce.order.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {

    @Mock
    private OutboxRepository outboxRepository;

    private OutboxService outboxService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        outboxService = new OutboxService(outboxRepository, objectMapper);
    }

    @Test
    void testPublishToOutbox_SuccessfullyCreatesOutboxRecord() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        BigDecimal totalAmount = BigDecimal.valueOf(99.99);
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, customerId, totalAmount);

        // Act
        outboxService.publishToOutbox(event, orderId.toString(), "Order");

        // Assert
        verify(outboxRepository).save(any(Outbox.class));
    }

    @Test
    void testMarkAsPublished_UpdatesOutboxRecord() {
        // Arrange
        Long outboxId = 1L;
        Outbox outbox = Outbox.builder()
            .id(outboxId)
            .aggregateId("order-123")
            .aggregateType("Order")
            .eventType("OrderCreatedEvent")
            .payload("{\"test\": \"data\"}")
            .timestamp(System.currentTimeMillis())
            .status(OutboxStatus.PENDING)
            .retryCount(0)
            .build();

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));

        // Act
        outboxService.markAsPublished(outboxId);

        // Assert
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(outbox.getPublishedAt()).isNotNull();
        verify(outboxRepository).save(outbox);
    }

    @Test
    void testRecordFailure_IncrementsRetryCount() {
        // Arrange
        Long outboxId = 1L;
        int maxRetries = 3;
        Outbox outbox = Outbox.builder()
            .id(outboxId)
            .aggregateId("order-123")
            .aggregateType("Order")
            .eventType("OrderCreatedEvent")
            .payload("{\"test\": \"data\"}")
            .timestamp(System.currentTimeMillis())
            .status(OutboxStatus.PENDING)
            .retryCount(0)
            .build();

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));

        // Act
        outboxService.recordFailure(outboxId, maxRetries);

        // Assert
        assertThat(outbox.getRetryCount()).isEqualTo(1);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING); // Not failed yet
        verify(outboxRepository).save(outbox);
    }

    @Test
    void testRecordFailure_MarksAsFailed_WhenMaxRetriesExceeded() {
        // Arrange
        Long outboxId = 1L;
        int maxRetries = 3;
        Outbox outbox = Outbox.builder()
            .id(outboxId)
            .aggregateId("order-123")
            .aggregateType("Order")
            .eventType("OrderCreatedEvent")
            .payload("{\"test\": \"data\"}")
            .timestamp(System.currentTimeMillis())
            .status(OutboxStatus.PENDING)
            .retryCount(2) // Already failed twice
            .build();

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));

        // Act
        outboxService.recordFailure(outboxId, maxRetries);

        // Assert
        assertThat(outbox.getRetryCount()).isEqualTo(3);
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.FAILED);
        verify(outboxRepository).save(outbox);
    }

    @Test
    void testMarkAsPublished_ThrowsException_WhenOutboxNotFound() {
        // Arrange
        Long outboxId = 999L;
        when(outboxRepository.findById(outboxId)).thenReturn(Optional.empty());

        // Act & Assert
        assertThatThrownBy(() -> outboxService.markAsPublished(outboxId))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Outbox event not found");
    }

    @Test
    void testCountPendingEvents_ReturnsCorrectCount() {
        // Arrange
        when(outboxRepository.countPendingEvents()).thenReturn(5L);

        // Act
        long count = outboxService.countPendingEvents();

        // Assert
        assertThat(count).isEqualTo(5L);
        verify(outboxRepository).countPendingEvents();
    }
}
