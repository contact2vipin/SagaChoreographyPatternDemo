package com.ecommerce.order.repository;

import com.ecommerce.order.entity.Outbox;
import com.ecommerce.order.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface OutboxRepository extends JpaRepository<Outbox, Long> {
    
    @Query("SELECT o FROM Outbox o WHERE o.status = :status ORDER BY o.createdAt ASC LIMIT :limit")
    List<Outbox> findPendingEvents(@Param("status") OutboxStatus status, @Param("limit") int limit);
    
    @Query("SELECT o FROM Outbox o WHERE o.status = 'PENDING' ORDER BY o.createdAt ASC LIMIT :limit")
    List<Outbox> findPendingEventsWithLimit(@Param("limit") int limit);
    
    Optional<Outbox> findByAggregateIdAndAggregateTypeAndEventType(
        String aggregateId,
        String aggregateType,
        String eventType
    );
    
    List<Outbox> findByStatusAndRetryCountLessThan(OutboxStatus status, int maxRetries);
    
    @Query("SELECT COUNT(o) FROM Outbox o WHERE o.status = 'PENDING'")
    long countPendingEvents();
    
    @Query("SELECT o FROM Outbox o WHERE o.status = 'PUBLISHED' AND o.publishedAt < :cutoffTime")
    List<Outbox> findOldPublishedEvents(@Param("cutoffTime") Long cutoffTime);
}
