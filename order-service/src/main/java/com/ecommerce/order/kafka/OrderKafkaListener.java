package com.ecommerce.order.kafka;

import com.ecommerce.order.config.KafkaTopicConfig;
import com.ecommerce.order.dto.InventoryFailedEvent;
import com.ecommerce.order.dto.OrderCancelledEvent;
import com.ecommerce.order.dto.OrderConfirmedEvent;
import com.ecommerce.order.dto.PaymentCompletedEvent;
import com.ecommerce.order.dto.PaymentFailedEvent;
import com.ecommerce.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderKafkaListener {

    private final OrderService orderService;
    private final OrderKafkaProducer orderKafkaProducer;

    /**
     * CONSUMES: inventory-failed
     * PRODUCES: order-cancelled
     * FLOW: Compensation flow triggered when inventory reservation fails.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_INVENTORY_FAILED,
            groupId = "${spring.application.name}",
            containerFactory = "inventoryFailedKafkaListenerContainerFactory")
    public void handleInventoryFailed(InventoryFailedEvent event) {
        try {
            orderService.cancelOrder(event.orderId(), event.reason());
            orderKafkaProducer.sendOrderCancelled(new OrderCancelledEvent(event.orderId(), event.reason()));
        } catch (Exception exception) {
            log.error("Failed to handle inventory failure for order {}", event.orderId(), exception);
        }
    }

    /**
     * CONSUMES: payment-completed
     * PRODUCES: order-confirmed
     * FLOW: Success flow that finalizes the saga after payment succeeds.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_PAYMENT_COMPLETED,
            groupId = "${spring.application.name}",
            containerFactory = "paymentCompletedKafkaListenerContainerFactory")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        try {
            orderService.confirmOrder(event.orderId());
            orderKafkaProducer.sendOrderConfirmed(new OrderConfirmedEvent(event.orderId()));
        } catch (Exception exception) {
            log.error("Failed to handle payment completion for order {}", event.orderId(), exception);
        }
    }

    /**
     * CONSUMES: payment-failed
     * PRODUCES: order-cancelled
     * FLOW: Compensation flow triggered when payment processing fails.
     */
    @KafkaListener(
            topics = KafkaTopicConfig.TOPIC_PAYMENT_FAILED,
            groupId = "${spring.application.name}",
            containerFactory = "paymentFailedKafkaListenerContainerFactory")
    public void handlePaymentFailed(PaymentFailedEvent event) {
        try {
            orderService.cancelOrder(event.orderId(), event.reason());
            orderKafkaProducer.sendOrderCancelled(new OrderCancelledEvent(event.orderId(), event.reason()));
        } catch (Exception exception) {
            log.error("Failed to handle payment failure for order {}", event.orderId(), exception);
        }
    }
}
