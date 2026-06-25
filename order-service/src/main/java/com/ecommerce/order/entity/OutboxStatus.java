package com.ecommerce.order.entity;

public enum OutboxStatus {
    PENDING,      // Event created, not yet published to Kafka
    PUBLISHED,    // Successfully published to Kafka
    FAILED        // Publishing failed after max retries
}
