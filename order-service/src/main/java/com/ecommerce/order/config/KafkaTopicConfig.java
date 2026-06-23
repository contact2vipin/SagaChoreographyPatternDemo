package com.ecommerce.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_ORDER_CREATED = "order-created";
    public static final String TOPIC_INVENTORY_RESERVED = "inventory-reserved";
    public static final String TOPIC_INVENTORY_FAILED = "inventory-failed";
    public static final String TOPIC_INVENTORY_RELEASED = "inventory-released";
    public static final String TOPIC_PAYMENT_COMPLETED = "payment-completed";
    public static final String TOPIC_PAYMENT_FAILED = "payment-failed";
    public static final String TOPIC_ORDER_CONFIRMED = "order-confirmed";
    public static final String TOPIC_ORDER_CANCELLED = "order-cancelled";

    @Bean
    NewTopic orderCreatedTopic() {
        return TopicBuilder.name(TOPIC_ORDER_CREATED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(TOPIC_INVENTORY_RESERVED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic inventoryFailedTopic() {
        return TopicBuilder.name(TOPIC_INVENTORY_FAILED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic inventoryReleasedTopic() {
        return TopicBuilder.name(TOPIC_INVENTORY_RELEASED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic paymentCompletedTopic() {
        return TopicBuilder.name(TOPIC_PAYMENT_COMPLETED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic paymentFailedTopic() {
        return TopicBuilder.name(TOPIC_PAYMENT_FAILED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic orderConfirmedTopic() {
        return TopicBuilder.name(TOPIC_ORDER_CONFIRMED).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic orderCancelledTopic() {
        return TopicBuilder.name(TOPIC_ORDER_CANCELLED).partitions(1).replicas(1).build();
    }
}
