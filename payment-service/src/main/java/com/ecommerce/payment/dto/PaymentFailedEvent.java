package com.ecommerce.payment.dto;

import java.util.UUID;

public record PaymentFailedEvent(UUID orderId, String reason) {
}
