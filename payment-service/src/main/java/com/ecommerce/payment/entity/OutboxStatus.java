package com.ecommerce.payment.entity;

public enum OutboxStatus {
    PENDING,      // Event created, not yet published to Kafka
    PUBLISHED,    // Successfully published to Kafka
    FAILED        // Publishing failed after max retries
}
