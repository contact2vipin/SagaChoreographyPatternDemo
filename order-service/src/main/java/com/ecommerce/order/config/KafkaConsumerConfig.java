package com.ecommerce.order.config;

import com.ecommerce.order.dto.InventoryFailedEvent;
import com.ecommerce.order.dto.PaymentCompletedEvent;
import com.ecommerce.order.dto.PaymentFailedEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    ConsumerFactory<String, InventoryFailedEvent> inventoryFailedConsumerFactory(KafkaProperties kafkaProperties) {
        return consumerFactory(kafkaProperties, InventoryFailedEvent.class);
    }

    @Bean
    ConsumerFactory<String, PaymentCompletedEvent> paymentCompletedConsumerFactory(KafkaProperties kafkaProperties) {
        return consumerFactory(kafkaProperties, PaymentCompletedEvent.class);
    }

    @Bean
    ConsumerFactory<String, PaymentFailedEvent> paymentFailedConsumerFactory(KafkaProperties kafkaProperties) {
        return consumerFactory(kafkaProperties, PaymentFailedEvent.class);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, InventoryFailedEvent> inventoryFailedKafkaListenerContainerFactory(
            ConsumerFactory<String, InventoryFailedEvent> consumerFactory) {
        return listenerContainerFactory(consumerFactory);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> paymentCompletedKafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentCompletedEvent> consumerFactory) {
        return listenerContainerFactory(consumerFactory);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, PaymentFailedEvent> paymentFailedKafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentFailedEvent> consumerFactory) {
        return listenerContainerFactory(consumerFactory);
    }

    private <T> ConsumerFactory<String, T> consumerFactory(KafkaProperties kafkaProperties, Class<T> eventType) {
        // Build properties from KafkaProperties without using deprecated buildConsumerProperties()
        Map<String, Object> properties = new HashMap<>();
        
        // Add Kafka properties
        if (kafkaProperties.getBootstrapServers() != null) {
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        }
        if (kafkaProperties.getConsumer().getGroupId() != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        }
        
        // Set consumer-specific config
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(eventType);
        jsonDeserializer.addTrustedPackages("com.ecommerce");
        jsonDeserializer.setUseTypeHeaders(false);
        return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), jsonDeserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));
        return factory;
    }
}
