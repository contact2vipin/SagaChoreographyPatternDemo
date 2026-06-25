package com.ecommerce.order.service;

import com.ecommerce.order.entity.Order;
import com.ecommerce.order.entity.OrderStatus;
import com.ecommerce.order.repository.OrderRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderServiceIdempotencyTest {

    @Mock
    private OrderRepository orderRepository;

    @InjectMocks
    private OrderService orderService;

    private UUID orderId;
    private UUID customerId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        orderId = UUID.randomUUID();
        customerId = UUID.randomUUID();
    }

    @Test
    void confirmOrderShouldBeIdempotent() {
        // Given - Order already CONFIRMED
        Order confirmedOrder = Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .status(OrderStatus.CONFIRMED)
                .totalAmount(BigDecimal.valueOf(100))
                .build();

        when(orderRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(confirmedOrder));

        // When - Confirm again
        orderService.confirmOrder(orderId);

        // Then - Should not update (idempotent)
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    void confirmOrderShouldUpdatePendingOrder() {
        // Given - Order is PENDING
        Order pendingOrder = Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .status(OrderStatus.PENDING)
                .totalAmount(BigDecimal.valueOf(100))
                .build();

        when(orderRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(pendingOrder));

        // When
        orderService.confirmOrder(orderId);

        // Then - Should update to CONFIRMED
        verify(orderRepository).save(any(Order.class));
    }

    @Test
    void cancelOrderShouldBeIdempotent() {
        // Given - Order already CANCELLED
        Order cancelledOrder = Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .status(OrderStatus.CANCELLED)
                .totalAmount(BigDecimal.valueOf(100))
                .build();

        when(orderRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(cancelledOrder));

        // When - Cancel again
        orderService.cancelOrder(orderId, "Duplicate cancellation");

        // Then - Should not update (idempotent)
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    void cancelOrderShouldUpdatePendingOrder() {
        // Given - Order is PENDING
        Order pendingOrder = Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .status(OrderStatus.PENDING)
                .totalAmount(BigDecimal.valueOf(100))
                .build();

        when(orderRepository.findByOrderId(orderId))
                .thenReturn(Optional.of(pendingOrder));

        // When
        orderService.cancelOrder(orderId, "Insufficient inventory");

        // Then - Should update to CANCELLED
        verify(orderRepository).save(any(Order.class));
    }

    @Test
    void cancelOrderShouldHandleNonexistentOrder() {
        // Given - Order does not exist
        when(orderRepository.findByOrderId(orderId))
                .thenReturn(Optional.empty());

        // When - Should not throw exception
        orderService.cancelOrder(orderId, "Test reason");

        // Then - No operations performed
        verify(orderRepository, never()).save(any(Order.class));
    }
}
