package com.ecommerce.inventory.service;

import com.ecommerce.inventory.entity.InventoryItem;
import com.ecommerce.inventory.entity.InventoryReservation;
import com.ecommerce.inventory.repository.InventoryRepository;
import com.ecommerce.inventory.repository.InventoryReservationRepository;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InventoryServiceIdempotencyTest {

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private InventoryReservationRepository inventoryReservationRepository;

    @InjectMocks
    private InventoryService inventoryService;

    private UUID orderId;
    private UUID productId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        orderId = UUID.randomUUID();
        productId = UUID.randomUUID();
    }

    @Test
    void reserveStockForOrderShouldBeIdempotent() {
        // Given - Reservation already exists
        InventoryReservation existingReservation = InventoryReservation.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(1)
                .released(false)
                .build();

        when(inventoryReservationRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(existingReservation));

        // When - Try to reserve again
        boolean result1 = inventoryService.reserveStockForOrder(orderId, productId, 1);
        boolean result2 = inventoryService.reserveStockForOrder(orderId, productId, 1);

        // Then - Both should return true (idempotent), no duplicate reservation
        assertTrue(result1);
        assertTrue(result2);
        verify(inventoryReservationRepository, times(2)).findByOrderId(orderId);
        verify(inventoryReservationRepository, never()).save(any(InventoryReservation.class));
    }

    @Test
    void releaseReservationShouldBeIdempotent() {
        // Given - Reservation marked as released
        InventoryReservation releasedReservation = InventoryReservation.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(1)
                .released(true)
                .build();

        when(inventoryReservationRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(releasedReservation));

        InventoryItem inventoryItem = InventoryItem.builder()
                .productId(productId)
                .quantity(100)
                .reserved(1)
                .build();

        when(inventoryRepository.findByProductId(productId))
                .thenReturn(Optional.of(inventoryItem));

        // When - Try to release again
        Optional<InventoryReservation> result1 = inventoryService.releaseReservation(orderId);
        Optional<InventoryReservation> result2 = inventoryService.releaseReservation(orderId);

        // Then - Both should succeed but not double-release
        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        verify(inventoryRepository, never()).save(any(InventoryItem.class));
    }

    @Test
    void releaseReservationShouldOnlyReleaseOnce() {
        // Given - Reservation exists and is NOT released
        InventoryReservation unreleaseReservation = InventoryReservation.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(1)
                .released(false)
                .build();

        when(inventoryReservationRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(unreleaseReservation));

        InventoryItem inventoryItem = InventoryItem.builder()
                .productId(productId)
                .quantity(100)
                .reserved(1)
                .build();

        when(inventoryRepository.findByProductId(productId))
                .thenReturn(Optional.of(inventoryItem));

        // When - Release once
        inventoryService.releaseReservation(orderId);

        // Then - Should decrement reserved count
        verify(inventoryRepository).save(any(InventoryItem.class));
    }

    @Test
    void releaseReservationShouldHandleNonexistentReservation() {
        // Given - No reservation exists
        when(inventoryReservationRepository.findByOrderId(orderId))
                .thenReturn(Optional.empty());

        // When - Try to release nonexistent reservation
        Optional<InventoryReservation> result = inventoryService.releaseReservation(orderId);

        // Then - Should return empty without errors
        assertFalse(result.isPresent());
        verify(inventoryRepository, never()).save(any(InventoryItem.class));
    }
}
