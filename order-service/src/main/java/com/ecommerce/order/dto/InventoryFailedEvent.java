package com.ecommerce.order.dto;

import java.util.UUID;

public record InventoryFailedEvent(UUID orderId, String reason) {
}
