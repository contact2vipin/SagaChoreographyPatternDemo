package com.ecommerce.payment.kafka;

import com.ecommerce.payment.config.KafkaTopicConfig;
import com.ecommerce.payment.dto.PaymentCompletedEvent;
import com.ecommerce.payment.dto.PaymentFailedEvent;
import com.ecommerce.payment.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OutboxService outboxService;

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendPaymentCompleted(PaymentCompletedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Payment");
        log.debug("Payment-completed event published to outbox for order {}", event.orderId());
    }

    /**
     * TRANSACTIONAL OUTBOX PATTERN: Events published to outbox instead of directly to Kafka
     */
    public void sendPaymentFailed(PaymentFailedEvent event) {
        outboxService.publishToOutbox(event, event.orderId().toString(), "Payment");
        log.debug("Payment-failed event published to outbox for order {}", event.orderId());
    }
}
