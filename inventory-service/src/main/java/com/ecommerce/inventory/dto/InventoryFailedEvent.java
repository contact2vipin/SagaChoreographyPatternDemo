package com.ecommerce.inventory.dto;

import java.util.UUID;

public record InventoryFailedEvent(UUID orderId, String reason) {
}
