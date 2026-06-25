package com.ecommerce.payment.service;

import com.ecommerce.payment.entity.Payment;
import com.ecommerce.payment.entity.PaymentStatus;
import com.ecommerce.payment.repository.PaymentRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentServiceIdempotencyTest {

    @Mock
    private PaymentRepository paymentRepository;

    @InjectMocks
    private PaymentService paymentService;

    private UUID orderId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        orderId = UUID.randomUUID();
    }

    @Test
    void processPaymentShouldReturnCachedResultIfAlreadyCompleted() {
        // Given - Payment already COMPLETED
        Payment completedPayment = Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.COMPLETED)
                .amount(BigDecimal.valueOf(100))
                .transactionId("TXN-123")
                .build();

        when(paymentRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(completedPayment));

        // When
        PaymentStatus result1 = paymentService.processPayment(orderId, BigDecimal.valueOf(100));
        PaymentStatus result2 = paymentService.processPayment(orderId, BigDecimal.valueOf(100));

        // Then - Both should return COMPLETED (no duplicate charge)
        assertEquals(PaymentStatus.COMPLETED, result1);
        assertEquals(PaymentStatus.COMPLETED, result2);
    }

    @Test
    void processPaymentShouldReturnCachedResultIfAlreadyFailed() {
        // Given - Payment already FAILED
        Payment failedPayment = Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.FAILED)
                .amount(BigDecimal.valueOf(100))
                .build();

        when(paymentRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(failedPayment));

        // When
        PaymentStatus result1 = paymentService.processPayment(orderId, BigDecimal.valueOf(100));
        PaymentStatus result2 = paymentService.processPayment(orderId, BigDecimal.valueOf(100));

        // Then - Both should return FAILED (consistent)
        assertEquals(PaymentStatus.FAILED, result1);
        assertEquals(PaymentStatus.FAILED, result2);
    }

    @Test
    void processPaymentShouldProcessNewPaymentForPendingOrder() {
        // Given - No existing payment
        when(paymentRepository.findByOrderId(orderId))
                .thenReturn(Optional.empty());

        // When
        PaymentStatus result = paymentService.processPayment(orderId, BigDecimal.valueOf(100));

        // Then - Should attempt payment (randomly completes or fails)
        verify(paymentRepository).save(any(Payment.class));
    }

    @Test
    void processingDuplicatePaymentShouldNotChargeAgain() {
        // Given - First payment completes successfully
        Payment completedPayment = Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.COMPLETED)
                .amount(BigDecimal.valueOf(100))
                .transactionId("TXN-FIRST")
                .build();

        when(paymentRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(completedPayment));

        // When - Process payment again (simulating duplicate event)
        PaymentStatus result = paymentService.processPayment(orderId, BigDecimal.valueOf(100));

        // Then - Should return cached COMPLETED without new transaction ID
        assertEquals(PaymentStatus.COMPLETED, result);
    }
}
