package com.ecommerce.order.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderCreatedEvent(UUID orderId, UUID customerId, BigDecimal totalAmount) {
}
