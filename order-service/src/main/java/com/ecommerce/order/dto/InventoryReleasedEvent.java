package com.ecommerce.order.dto;

import java.util.UUID;

public record InventoryReleasedEvent(UUID orderId, UUID productId, Integer quantity) {
}
