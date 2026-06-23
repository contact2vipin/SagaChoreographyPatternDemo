package com.ecommerce.order.repository;

import com.ecommerce.order.entity.Order;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    Optional<Order> findByOrderId(UUID orderId);
    Optional<List<Order>> findByCustomerId(UUID customerId);
}
