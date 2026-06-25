package com.ecommerce.payment.kafka;

import com.ecommerce.payment.config.KafkaTopicConfig;
import com.ecommerce.payment.dto.InventoryReservedEvent;
import com.ecommerce.payment.dto.PaymentCompletedEvent;
import com.ecommerce.payment.dto.PaymentFailedEvent;
import com.ecommerce.payment.entity.Payment;
import com.ecommerce.payment.entity.PaymentStatus;
import com.ecommerce.payment.service.EventDeduplicationService;
import com.ecommerce.payment.service.PaymentService;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentKafkaListener {

    private static final BigDecimal DEMO_UNIT_PRICE = new BigDecimal("50.00");
    private static final String EVENT_TYPE = "INVENTORY_RESERVED";

    private final PaymentService paymentService;
    private final PaymentKafkaProducer paymentKafkaProducer;
    private final EventDeduplicationService eventDeduplicationService;

    /**
     * CONSUMES: inventory-reserved
     * PRODUCES: payment-completed or payment-failed
     * FLOW: Success path completes payment, failure path starts compensation.
     * IDEMPOTENCY: Skips processing if event already handled.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_INVENTORY_RESERVED,
            groupId = "${spring.application.name}",
            containerFactory = "inventoryReservedKafkaListenerContainerFactory")
    public void handleInventoryReserved(InventoryReservedEvent event) {
        // IDEMPOTENCY CHECK: Skip if already processed
        if (eventDeduplicationService.hasProcessedEvent(event.orderId(), EVENT_TYPE)) {
            log.info("Skipping duplicate inventory-reserved event for order {}", event.orderId());
            return;
        }

        try {
            BigDecimal amount = DEMO_UNIT_PRICE.multiply(BigDecimal.valueOf(event.quantity()));
            PaymentStatus paymentStatus = paymentService.processPayment(event.orderId(), amount);
            Payment payment = paymentService.findByOrderId(event.orderId())
                    .orElseThrow(() -> new IllegalStateException("Payment record not found for order " + event.orderId()));

            // Record event as processed
            eventDeduplicationService.recordProcessedEvent(event.orderId(), EVENT_TYPE, 
                    "Payment status: " + paymentStatus);

            if (paymentStatus == PaymentStatus.COMPLETED) {
                paymentKafkaProducer.sendPaymentCompleted(new PaymentCompletedEvent(
                        payment.getOrderId(),
                        payment.getTransactionId(),
                        payment.getAmount()));
            } else {
                paymentKafkaProducer.sendPaymentFailed(new PaymentFailedEvent(
                        payment.getOrderId(),
                        "Payment processing failed for order " + payment.getOrderId()));
            }
        } catch (Exception exception) {
            log.error("Failed to handle inventory reservation for order {}", event.orderId(), exception);
        }
    }
}
