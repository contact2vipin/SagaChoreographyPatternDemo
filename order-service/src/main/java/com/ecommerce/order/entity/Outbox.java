package com.ecommerce.order.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Entity
@Table(name = "outbox", indexes = {
    @Index(name = "idx_outbox_published", columnList = "published_at"),
    @Index(name = "idx_outbox_created", columnList = "created_at"),
    @Index(name = "idx_outbox_aggregate", columnList = "aggregate_id, aggregate_type")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Outbox {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, length = 255)
    private String aggregateId;      // Order ID
    
    @Column(nullable = false, length = 100)
    private String aggregateType;    // "Order"
    
    @Column(nullable = false, length = 255)
    private String eventType;        // Event class name
    
    @Column(nullable = false, columnDefinition = "LONGTEXT")
    private String payload;          // Serialized JSON event
    
    @Column(nullable = false)
    private Long timestamp;          // Event creation timestamp
    
    @Column(name = "published_at")
    private Long publishedAt;        // NULL if not published
    
    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;
    
    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();
    
    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();
    
    @PreUpdate
    public void preUpdate() {
        this.updatedAt = Instant.now();
    }
}
