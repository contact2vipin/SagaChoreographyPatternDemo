package com.ecommerce.inventory.kafka;

import com.ecommerce.inventory.config.KafkaTopicConfig;
import com.ecommerce.inventory.dto.InventoryFailedEvent;
import com.ecommerce.inventory.dto.InventoryReleasedEvent;
import com.ecommerce.inventory.dto.InventoryReservedEvent;
import com.ecommerce.inventory.dto.OrderCreatedEvent;
import com.ecommerce.inventory.dto.PaymentFailedEvent;
import com.ecommerce.inventory.entity.InventoryReservation;
import com.ecommerce.inventory.service.EventDeduplicationService;
import com.ecommerce.inventory.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryKafkaListener {

    private static final String ORDER_CREATED_EVENT_TYPE = "ORDER_CREATED";
    private static final String PAYMENT_FAILED_EVENT_TYPE = "PAYMENT_FAILED";

    private final InventoryService inventoryService;
    private final InventoryKafkaProducer inventoryKafkaProducer;
    private final EventDeduplicationService eventDeduplicationService;

    /**
     * CONSUMES: order-created
     * PRODUCES: inventory-reserved or inventory-failed
     * FLOW: Success path reserves stock, failure path triggers compensation for the saga.
     * IDEMPOTENCY: Skips processing if event already handled.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_ORDER_CREATED,
            groupId = "${spring.application.name}",
            containerFactory = "orderCreatedKafkaListenerContainerFactory")
    public void handleOrderCreated(OrderCreatedEvent event) {
        // IDEMPOTENCY CHECK: Skip if already processed
        if (eventDeduplicationService.hasProcessedEvent(event.orderId(), ORDER_CREATED_EVENT_TYPE)) {
            log.info("Skipping duplicate order-created event for order {}", event.orderId());
            return;
        }

        try {
            boolean reserved = inventoryService.reserveStockForOrder(
                    event.orderId(),
                    InventoryService.DEMO_PRODUCT_ID,
                    InventoryService.DEFAULT_ORDER_QUANTITY);

            // Record event as processed
            eventDeduplicationService.recordProcessedEvent(event.orderId(), ORDER_CREATED_EVENT_TYPE,
                    "Reservation: " + (reserved ? "SUCCESS" : "FAILED"));

            if (reserved) {
                inventoryKafkaProducer.sendInventoryReserved(new InventoryReservedEvent(
                        event.orderId(),
                        InventoryService.DEMO_PRODUCT_ID,
                        InventoryService.DEFAULT_ORDER_QUANTITY));
            } else {
                inventoryKafkaProducer.sendInventoryFailed(new InventoryFailedEvent(
                        event.orderId(),
                        "Insufficient stock for product " + InventoryService.DEMO_PRODUCT_ID));
            }
        } catch (Exception exception) {
            log.error("Failed to handle order-created event for order {}", event.orderId(), exception);
        }
    }

    /**
     * CONSUMES: payment-failed
     * PRODUCES: inventory-released
     * FLOW: Compensation flow that restores stock after downstream payment failure.
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
            inventoryService.releaseReservation(event.orderId()).ifPresentOrElse(
                    reservation -> {
                        // Record event as processed
                        eventDeduplicationService.recordProcessedEvent(event.orderId(), PAYMENT_FAILED_EVENT_TYPE,
                                "Released: " + reservation.getQuantity() + " units");
                        inventoryKafkaProducer.sendInventoryReleased(new InventoryReleasedEvent(
                                reservation.getOrderId(),
                                reservation.getProductId(),
                                reservation.getQuantity()));
                    },
                    () -> {
                        log.warn("No reservation found for order {} during compensation", event.orderId());
                        // Still record as processed to avoid retries
                        eventDeduplicationService.recordProcessedEvent(event.orderId(), PAYMENT_FAILED_EVENT_TYPE,
                                "No reservation found");
                    });
        } catch (Exception exception) {
            log.error("Failed to compensate inventory for order {}", event.orderId(), exception);
        }
    }
}
