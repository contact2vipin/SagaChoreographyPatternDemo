package com.ecommerce.order.service;

import com.ecommerce.order.dto.OrderDetails;
import com.ecommerce.order.entity.Order;
import com.ecommerce.order.entity.OrderStatus;
import com.ecommerce.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;

    @Transactional
    public Order createOrder(UUID customerId, BigDecimal totalAmount) {
        Order order = Order.builder()
                .orderId(UUID.randomUUID())
                .customerId(customerId)
                .status(OrderStatus.PENDING)
                .totalAmount(totalAmount)
                .build();

        Order savedOrder = orderRepository.save(order);
        log.info("Created order {} for customer {}", savedOrder.getOrderId(), customerId);
        return savedOrder;
    }

    @Transactional
    public void confirmOrder(UUID orderId) {
        orderRepository.findByOrderId(orderId).ifPresentOrElse(order -> {
            if (order.getStatus() == OrderStatus.CONFIRMED) {
                log.info("Order {} is already confirmed", orderId);
                return;
            }
            order.setStatus(OrderStatus.CONFIRMED);
            orderRepository.save(order);
            log.info("Order {} confirmed", orderId);
        }, () -> log.warn("Order {} not found for confirmation", orderId));
    }

    @Transactional
    public void cancelOrder(UUID orderId, String reason) {
        // COMPENSATING TRANSACTION: Cancels the order after a downstream saga failure.
        orderRepository.findByOrderId(orderId).ifPresentOrElse(order -> {
            if (order.getStatus() == OrderStatus.CANCELLED) {
                log.info("Order {} is already cancelled", orderId);
                return;
            }
            order.setStatus(OrderStatus.CANCELLED);
            orderRepository.save(order);
            log.warn("Order {} cancelled because: {}", orderId, reason);
        }, () -> log.warn("Order {} not found for cancellation. Reason: {}", orderId, reason));
    }

    public List<OrderDetails> getAllOrders(UUID customerId) {
        List<Order> orders = orderRepository.findByCustomerId(customerId)
                .orElseThrow(() -> new RuntimeException("No order exists for this customer!"));
        return orders.stream()
                .map(OrderDetails::fromEntity)
                .toList();
    }

    public OrderDetails getOrderById(UUID orderId) {
        Order order = orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("No order exists for this customer!"));
        return OrderDetails.fromEntity(order);
    }
}
