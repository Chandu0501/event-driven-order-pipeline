package com.chandu.orderapi.controller;

import com.chandu.orderapi.domain.Order;
import com.chandu.orderapi.service.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Tag(name = "Order Pipeline", description = "Event-driven order management endpoints")
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    @Operation(summary = "Create a new order", description = "Creates an order and publishes ORDER_CREATED event to Kafka")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Order created"),
        @ApiResponse(responseCode = "400", description = "Invalid request payload"),
        @ApiResponse(responseCode = "503", description = "Service unavailable")
    })
    public ResponseEntity<Order> createOrder(@Valid @RequestBody Order order) {
        log.info("POST /api/v1/orders - customerId={}", order.getCustomerId());
        Order created = orderService.createOrder(order);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping("/{orderId}")
    @Operation(summary = "Get order by ID")
    @ApiResponse(responseCode = "200", description = "Order found")
    @ApiResponse(responseCode = "404", description = "Order not found")
    public ResponseEntity<Order> getOrder(
            @Parameter(description = "Unique order ID") @PathVariable String orderId) {
        return ResponseEntity.ok(orderService.getOrder(orderId));
    }

    @GetMapping("/customer/{customerId}")
    @Operation(summary = "Get all orders for a customer")
    public ResponseEntity<List<Order>> getOrdersByCustomer(@PathVariable String customerId) {
        return ResponseEntity.ok(orderService.getOrdersByCustomer(customerId));
    }

    @PostMapping("/{orderId}/validate-payment")
    @Operation(summary = "Trigger payment validation for an order")
    public ResponseEntity<Order> validatePayment(@PathVariable String orderId) {
        log.info("POST /api/v1/orders/{}/validate-payment", orderId);
        return ResponseEntity.ok(orderService.validatePayment(orderId));
    }

    @PostMapping("/{orderId}/ship")
    @Operation(summary = "Mark an order as shipped")
    public ResponseEntity<Order> shipOrder(@PathVariable String orderId) {
        log.info("POST /api/v1/orders/{}/ship", orderId);
        return ResponseEntity.ok(orderService.shipOrder(orderId));
    }
}
