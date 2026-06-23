package com.ecommerce.payment.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentCompletedEvent(UUID orderId, String transactionId, BigDecimal amount) {
}
