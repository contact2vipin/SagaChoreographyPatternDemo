package com.ecommerce.order.dto;

import java.util.UUID;

public record PaymentFailedEvent(UUID orderId, String reason) {
}
