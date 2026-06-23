package com.ecommerce.inventory.service;

import com.ecommerce.inventory.entity.InventoryItem;
import com.ecommerce.inventory.entity.InventoryReservation;
import com.ecommerce.inventory.repository.InventoryRepository;
import com.ecommerce.inventory.repository.InventoryReservationRepository;
import jakarta.annotation.PostConstruct;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    public static final UUID DEMO_PRODUCT_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    public static final int DEFAULT_ORDER_QUANTITY = 1;

    private final InventoryRepository inventoryRepository;
    private final InventoryReservationRepository inventoryReservationRepository;

    @PostConstruct
    @Transactional
    public void initializeDemoInventory() {
        inventoryRepository.findByProductId(DEMO_PRODUCT_ID).orElseGet(() -> inventoryRepository.save(InventoryItem.builder()
                .productId(DEMO_PRODUCT_ID)
                .quantity(100)
                .reserved(0)
                .build()));
    }

    @Transactional
    public boolean reserveStock(UUID productId, Integer quantity) {
        InventoryItem inventoryItem = inventoryRepository.findByProductId(productId)
                .orElseGet(() -> inventoryRepository.save(InventoryItem.builder()
                        .productId(productId)
                        .quantity(0)
                        .reserved(0)
                        .build()));

        int availableQuantity = inventoryItem.getQuantity() - inventoryItem.getReserved();
        if (availableQuantity < quantity) {
            log.warn("Insufficient stock for product {}. Requested={}, available={}", productId, quantity, availableQuantity);
            return false;
        }

        inventoryItem.setReserved(inventoryItem.getReserved() + quantity);
        inventoryRepository.save(inventoryItem);
        log.info("Reserved {} units for product {}", quantity, productId);
        return true;
    }

    @Transactional
    public boolean reserveStockForOrder(UUID orderId, UUID productId, Integer quantity) {
        if (inventoryReservationRepository.findByOrderId(orderId).isPresent()) {
            log.info("Inventory already reserved for order {}", orderId);
            return true;
        }

        boolean reserved = reserveStock(productId, quantity);
        if (reserved) {
            inventoryReservationRepository.save(InventoryReservation.builder()
                    .orderId(orderId)
                    .productId(productId)
                    .quantity(quantity)
                    .released(false)
                    .build());
        }
        return reserved;
    }

    @Transactional
    public void releaseStock(UUID productId, Integer quantity) {
        // COMPENSATING TRANSACTION: Releases previously reserved stock after payment failure.
        inventoryRepository.findByProductId(productId).ifPresent(inventoryItem -> {
            inventoryItem.setReserved(Math.max(0, inventoryItem.getReserved() - quantity));
            inventoryRepository.save(inventoryItem);
            log.info("Released {} reserved units for product {}", quantity, productId);
        });
    }

    @Transactional
    public Optional<InventoryReservation> releaseReservation(UUID orderId) {
        // COMPENSATING TRANSACTION: Restores stock for a saga by using the persisted reservation.
        return inventoryReservationRepository.findByOrderId(orderId).map(reservation -> {
            if (!reservation.isReleased()) {
                releaseStock(reservation.getProductId(), reservation.getQuantity());
                reservation.setReleased(true);
                inventoryReservationRepository.save(reservation);
            }
            return reservation;
        });
    }
}
