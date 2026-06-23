package com.ecommerce.order.kafka;

import com.ecommerce.order.config.KafkaTopicConfig;
import com.ecommerce.order.dto.OrderCancelledEvent;
import com.ecommerce.order.dto.OrderConfirmedEvent;
import com.ecommerce.order.dto.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrderCreated(OrderCreatedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_ORDER_CREATED, event.orderId().toString(), event);
        log.info("Published order-created event for order {}", event.orderId());
    }

    public void sendOrderConfirmed(OrderConfirmedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_ORDER_CONFIRMED, event.orderId().toString(), event);
        log.info("Published order-confirmed event for order {}", event.orderId());
    }

    public void sendOrderCancelled(OrderCancelledEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_ORDER_CANCELLED, event.orderId().toString(), event);
        log.info("Published order-cancelled event for order {}", event.orderId());
    }
}
