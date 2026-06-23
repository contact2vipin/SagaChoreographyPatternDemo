package com.ecommerce.payment.kafka;

import com.ecommerce.payment.config.KafkaTopicConfig;
import com.ecommerce.payment.dto.PaymentCompletedEvent;
import com.ecommerce.payment.dto.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendPaymentCompleted(PaymentCompletedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_PAYMENT_COMPLETED, event.orderId().toString(), event);
        log.info("Published payment-completed event for order {}", event.orderId());
    }

    public void sendPaymentFailed(PaymentFailedEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_PAYMENT_FAILED, event.orderId().toString(), event);
        log.info("Published payment-failed event for order {}", event.orderId());
    }
}
