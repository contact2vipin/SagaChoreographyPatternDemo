package com.ecommerce.inventory.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class OutboxConfig {
    // Scheduling is enabled via @EnableScheduling annotation
    // Configuration is loaded from application.yml
    // Properties:
    // - outbox.polling.enabled: true
    // - outbox.polling.interval-ms: 2000
    // - outbox.polling.batch-size: 100
    // - outbox.polling.max-retries: 3
}
