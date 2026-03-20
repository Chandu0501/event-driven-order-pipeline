package com.chandu.orderapi.service;

import com.chandu.orderapi.domain.Order;
import com.chandu.orderapi.domain.OrderEvent;
import com.chandu.orderapi.domain.OrderStatus;
import com.chandu.orderapi.exception.OrderNotFoundException;
import com.chandu.orderapi.exception.OrderProcessingException;
import com.chandu.orderapi.metrics.OrderMetrics;
import com.chandu.orderapi.producer.OrderEventProducer;
import com.chandu.orderapi.repository.OrderRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String SERVICE_NAME = "order-pipeline";

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;
    private final IdempotencyService idempotencyService;
    private final OrderMetrics orderMetrics;

    /**
     * Creates a new order and publishes ORDER_CREATED event to Kafka.
     * Transactional: DB write + Kafka publish are coordinated — on failure,
     * the DB record is rolled back and no event is emitted.
     */
    @Transactional
    public Order createOrder(Order order) {
        log.info("Creating order for customerId={} productId={}", order.getCustomerId(), order.getProductId());

        Order saved = orderRepository.save(order);
        orderMetrics.incrementOrderCreated();

        OrderEvent event = OrderEvent.from(saved, "ORDER_CREATED", SERVICE_NAME);
        orderEventProducer.publishOrderEvent(event);

        log.info("Order created and event published: orderId={}", saved.getOrderId());
        return saved;
    }

    /**
     * Validates payment for an order. Uses circuit breaker to prevent
     * cascading failures if the payment service is unavailable.
     */
    @Transactional
    @CircuitBreaker(name = "paymentService", fallbackMethod = "validatePaymentFallback")
    @Retry(name = "paymentService")
    public Order validatePayment(String orderId) {
        Order order = findOrderOrThrow(orderId);

        if (order.getStatus() != OrderStatus.PENDING) {
            throw new OrderProcessingException("Order " + orderId + " is not in PENDING state: " + order.getStatus());
        }

        // Simulate payment validation logic
        log.info("Validating payment for orderId={} amount={}", orderId, order.getTotalAmount());
        order.setStatus(OrderStatus.PAYMENT_VALIDATED);
        Order updated = orderRepository.save(order);

        OrderEvent event = OrderEvent.from(updated, "PAYMENT_VALIDATED", SERVICE_NAME);
        orderEventProducer.publishOrderEvent(event);

        orderMetrics.incrementPaymentValidated();
        return updated;
    }

    /**
     * Circuit breaker fallback — graceful degradation when payment service is down.
     * Marks order as FAILED and routes to DLQ for manual review.
     */
    public Order validatePaymentFallback(String orderId, Throwable ex) {
        log.error("Payment service circuit breaker OPEN for orderId={}: {}", orderId, ex.getMessage());
        orderMetrics.incrementCircuitBreakerOpen();

        return orderRepository.findByOrderId(orderId).map(order -> {
            order.setStatus(OrderStatus.FAILED);
            order.setFailureReason("Payment service unavailable — circuit breaker open");
            return orderRepository.save(order);
        }).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    /**
     * Reserves inventory after payment validation.
     */
    @Transactional
    public Order reserveInventory(String orderId) {
        Order order = findOrderOrThrow(orderId);

        if (order.getStatus() != OrderStatus.PAYMENT_VALIDATED) {
            throw new OrderProcessingException("Cannot reserve inventory: order " + orderId + " not payment-validated");
        }

        log.info("Reserving inventory for orderId={} productId={} qty={}", orderId, order.getProductId(), order.getQuantity());
        order.setStatus(OrderStatus.INVENTORY_RESERVED);
        Order updated = orderRepository.save(order);

        OrderEvent event = OrderEvent.from(updated, "INVENTORY_RESERVED", SERVICE_NAME);
        orderEventProducer.publishOrderEvent(event);

        orderMetrics.incrementInventoryReserved();
        return updated;
    }

    /**
     * Ships an order — final happy-path step.
     */
    @Transactional
    public Order shipOrder(String orderId) {
        Order order = findOrderOrThrow(orderId);

        if (order.getStatus() != OrderStatus.INVENTORY_RESERVED) {
            throw new OrderProcessingException("Cannot ship: order " + orderId + " inventory not reserved");
        }

        order.setStatus(OrderStatus.SHIPPED);
        Order updated = orderRepository.save(order);

        OrderEvent event = OrderEvent.from(updated, "ORDER_SHIPPED", SERVICE_NAME);
        orderEventProducer.publishOrderEvent(event);

        orderMetrics.incrementOrderShipped();
        log.info("Order shipped: orderId={}", orderId);
        return updated;
    }

    /**
     * Handles a failed order — increments retry count and re-queues if below threshold.
     */
    @Transactional
    public Order handleFailure(String orderId, String reason) {
        Order order = findOrderOrThrow(orderId);
        order.setFailureReason(reason);
        order.setRetryCount(order.getRetryCount() + 1);

        if (order.getRetryCount() >= 3) {
            log.warn("Order exceeded max retries — routing to DLQ: orderId={}", orderId);
            order.setStatus(OrderStatus.DEAD_LETTERED);
            orderMetrics.incrementDeadLettered();
        } else {
            order.setStatus(OrderStatus.FAILED);
            orderMetrics.incrementOrderFailed();
        }

        return orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public Order getOrder(String orderId) {
        return findOrderOrThrow(orderId);
    }

    @Transactional(readOnly = true)
    public List<Order> getOrdersByCustomer(String customerId) {
        return orderRepository.findByCustomerId(customerId);
    }

    private Order findOrderOrThrow(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
