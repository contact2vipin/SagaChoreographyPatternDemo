package com.ecommerce.payment.service;

import com.ecommerce.payment.entity.Payment;
import com.ecommerce.payment.entity.PaymentStatus;
import com.ecommerce.payment.repository.PaymentRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;

    @Transactional
    public PaymentStatus processPayment(UUID orderId, BigDecimal amount) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElse(Payment.builder().orderId(orderId).status(PaymentStatus.PENDING).build());

        // Idempotency: If already processed, return cached result without charging again
        if (payment.getStatus() == PaymentStatus.COMPLETED) {
            log.info("Payment already completed for order {}. Returning cached result.", orderId);
            return PaymentStatus.COMPLETED;
        }

        if (payment.getStatus() == PaymentStatus.FAILED) {
            log.info("Payment already failed for order {}. Returning cached result.", orderId);
            return PaymentStatus.FAILED;
        }

        boolean success = ThreadLocalRandom.current().nextInt(10) < 8;
        payment.setAmount(amount);

        if (success) {
            payment.setStatus(PaymentStatus.COMPLETED);
            payment.setTransactionId("TXN-" + UUID.randomUUID());
            log.info("Payment completed for order {}", orderId);
        } else {
            payment.setStatus(PaymentStatus.FAILED);
            payment.setTransactionId(null);
            log.warn("Payment failed for order {}", orderId);
        }

        paymentRepository.save(payment);
        return payment.getStatus();
    }

    @Transactional(readOnly = true)
    public Optional<Payment> findByOrderId(UUID orderId) {
        return paymentRepository.findByOrderId(orderId);
    }
}
