package com.ecommerce.order.dto;

import java.util.UUID;

public record OrderCancelledEvent(UUID orderId, String reason) {
}
