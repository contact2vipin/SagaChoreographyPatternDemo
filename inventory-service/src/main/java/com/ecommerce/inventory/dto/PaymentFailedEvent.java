package com.ecommerce.inventory.dto;

import java.util.UUID;

public record PaymentFailedEvent(UUID orderId, String reason) {
}
