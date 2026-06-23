package com.ecommerce.order.controller;

import com.ecommerce.order.dto.OrderCreatedEvent;
import com.ecommerce.order.dto.OrderDetails;
import com.ecommerce.order.entity.Order;
import com.ecommerce.order.kafka.OrderKafkaProducer;
import com.ecommerce.order.service.OrderService;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final OrderKafkaProducer orderKafkaProducer;

    @PostMapping("/orders")
    public ResponseEntity<Order> createOrder(@RequestBody CreateOrderRequest request) {
        Order order = orderService.createOrder(request.customerId(), request.totalAmount());
        orderKafkaProducer.sendOrderCreated(new OrderCreatedEvent(
                order.getOrderId(),
                order.getCustomerId(),
                order.getTotalAmount()));
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("customers/{id}")
    public ResponseEntity<List<OrderDetails>> getAllOrders(@PathVariable("id") UUID customerId) {
        List<OrderDetails> orders = orderService.getAllOrders(customerId);
        return ResponseEntity.status(HttpStatus.OK).body(orders);
    }

    @GetMapping("orders/{id}")
    public ResponseEntity<OrderDetails> getOrderById(@PathVariable("id") UUID orderId) {
        OrderDetails order = orderService.getOrderById(orderId);
        return ResponseEntity.status(HttpStatus.OK).body(order);
    }

    public record CreateOrderRequest(UUID customerId, BigDecimal totalAmount) {
    }
}
