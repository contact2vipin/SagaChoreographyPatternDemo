package com.ecommerce.order.kafka;

import com.ecommerce.order.config.KafkaTopicConfig;
import com.ecommerce.order.dto.InventoryFailedEvent;
import com.ecommerce.order.dto.OrderCancelledEvent;
import com.ecommerce.order.dto.OrderConfirmedEvent;
import com.ecommerce.order.dto.PaymentCompletedEvent;
import com.ecommerce.order.dto.PaymentFailedEvent;
import com.ecommerce.order.service.EventDeduplicationService;
import com.ecommerce.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderKafkaListener {

    private static final String INVENTORY_FAILED_EVENT_TYPE = "INVENTORY_FAILED";
    private static final String PAYMENT_COMPLETED_EVENT_TYPE = "PAYMENT_COMPLETED";
    private static final String PAYMENT_FAILED_EVENT_TYPE = "PAYMENT_FAILED";

    private final OrderService orderService;
    private final OrderKafkaProducer orderKafkaProducer;
    private final EventDeduplicationService eventDeduplicationService;

    /**
     * CONSUMES: inventory-failed
     * PRODUCES: order-cancelled
     * FLOW: Compensation flow triggered when inventory reservation fails.
     * IDEMPOTENCY: Skips processing if event already handled.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_INVENTORY_FAILED,
            groupId = "${spring.application.name}",
            containerFactory = "inventoryFailedKafkaListenerContainerFactory")
    public void handleInventoryFailed(InventoryFailedEvent event) {
        // IDEMPOTENCY CHECK: Skip if already processed
        if (eventDeduplicationService.hasProcessedEvent(event.orderId(), INVENTORY_FAILED_EVENT_TYPE)) {
            log.info("Skipping duplicate inventory-failed event for order {}", event.orderId());
            return;
        }

        try {
            orderService.cancelOrder(event.orderId(), event.reason());
            
            // Record event as processed
            eventDeduplicationService.recordProcessedEvent(event.orderId(), INVENTORY_FAILED_EVENT_TYPE,
                    "Order cancelled due to: " + event.reason());
            
            orderKafkaProducer.sendOrderCancelled(new OrderCancelledEvent(event.orderId(), event.reason()));
        } catch (Exception exception) {
            log.error("Failed to handle inventory failure for order {}", event.orderId(), exception);
        }
    }

    /**
     * CONSUMES: payment-completed
     * PRODUCES: order-confirmed
     * FLOW: Success flow that finalizes the saga after payment succeeds.
     * IDEMPOTENCY: Skips processing if event already handled.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_PAYMENT_COMPLETED,
            groupId = "${spring.application.name}",
            containerFactory = "paymentCompletedKafkaListenerContainerFactory")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        // IDEMPOTENCY CHECK: Skip if already processed
        if (eventDeduplicationService.hasProcessedEvent(event.orderId(), PAYMENT_COMPLETED_EVENT_TYPE)) {
            log.info("Skipping duplicate payment-completed event for order {}", event.orderId());
            return;
        }

        try {
            orderService.confirmOrder(event.orderId());
            
            // Record event as processed
            eventDeduplicationService.recordProcessedEvent(event.orderId(), PAYMENT_COMPLETED_EVENT_TYPE,
                    "Transaction: " + event.transactionId());
            
            orderKafkaProducer.sendOrderConfirmed(new OrderConfirmedEvent(event.orderId()));
        } catch (Exception exception) {
            log.error("Failed to handle payment completion for order {}", event.orderId(), exception);
        }
    }

    /**
     * CONSUMES: payment-failed
     * PRODUCES: order-cancelled
     * FLOW: Compensation flow triggered when payment processing fails.
     * IDEMPOTENCY: Skips processing if event already handled.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_PAYMENT_FAILED,
            groupId = "${spring.application.name}",
            containerFactory = "paymentFailedKafkaListenerContainerFactory")
    public void handlePaymentFailed(PaymentFailedEvent event) {
        // IDEMPOTENCY CHECK: Skip if already processed
        if (eventDeduplicationService.hasProcessedEvent(event.orderId(), PAYMENT_FAILED_EVENT_TYPE)) {
            log.info("Skipping duplicate payment-failed event for order {}", event.orderId());
            return;
        }

        try {
            orderService.cancelOrder(event.orderId(), event.reason());
            
            // Record event as processed
            eventDeduplicationService.recordProcessedEvent(event.orderId(), PAYMENT_FAILED_EVENT_TYPE,
                    "Order cancelled due to: " + event.reason());
            
            orderKafkaProducer.sendOrderCancelled(new OrderCancelledEvent(event.orderId(), event.reason()));
        } catch (Exception exception) {
            log.error("Failed to handle payment failure for order {}", event.orderId(), exception);
        }
    }
}
