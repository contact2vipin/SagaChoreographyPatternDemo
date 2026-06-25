package com.ecommerce.inventory.repository;

import com.ecommerce.inventory.entity.ProcessedEvent;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    Optional<ProcessedEvent> findByEventKey(String eventKey);
}
