package com.ecommerce.payment.dto;

import java.util.UUID;

public record InventoryReservedEvent(UUID orderId, UUID productId, Integer quantity) {
}
