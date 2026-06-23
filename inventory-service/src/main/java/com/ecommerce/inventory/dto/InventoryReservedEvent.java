package com.ecommerce.inventory.dto;

import java.util.UUID;

public record InventoryReservedEvent(UUID orderId, UUID productId, Integer quantity) {
}
