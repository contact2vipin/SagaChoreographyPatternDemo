package com.ecommerce.order.dto;

import java.util.UUID;

public record OrderConfirmedEvent(UUID orderId) {
}
