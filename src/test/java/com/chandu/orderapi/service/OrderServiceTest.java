package com.chandu.orderapi.service;

import com.chandu.orderapi.domain.Order;
import com.chandu.orderapi.domain.OrderStatus;
import com.chandu.orderapi.exception.OrderNotFoundException;
import com.chandu.orderapi.exception.OrderProcessingException;
import com.chandu.orderapi.metrics.OrderMetrics;
import com.chandu.orderapi.producer.OrderEventProducer;
import com.chandu.orderapi.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderService Unit Tests")
class OrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private OrderEventProducer orderEventProducer;
    @Mock private IdempotencyService idempotencyService;
    @Mock private OrderMetrics orderMetrics;

    @InjectMocks
    private OrderService orderService;

    private Order testOrder;

    @BeforeEach
    void setUp() {
        testOrder = Order.builder()
                .orderId("order-001")
                .customerId("cust-123")
                .productId("prod-456")
                .quantity(2)
                .totalAmount(new BigDecimal("99.99"))
                .status(OrderStatus.PENDING)
                .retryCount(0)
                .build();
    }

    @Nested
    @DisplayName("createOrder")
    class CreateOrderTests {

        @Test
        @DisplayName("should save order and publish ORDER_CREATED event")
        void createOrder_success() {
            when(orderRepository.save(any(Order.class))).thenReturn(testOrder);

            Order result = orderService.createOrder(testOrder);

            assertThat(result).isNotNull();
            assertThat(result.getOrderId()).isEqualTo("order-001");

            verify(orderRepository, times(1)).save(testOrder);
            verify(orderEventProducer, times(1)).publishOrderEvent(
                    argThat(e -> "ORDER_CREATED".equals(e.getEventType())
                            && "order-001".equals(e.getOrderId()))
            );
            verify(orderMetrics).incrementOrderCreated();
        }

        @Test
        @DisplayName("should propagate exception if repository save fails")
        void createOrder_repositoryFailure_throwsException() {
            when(orderRepository.save(any())).thenThrow(new RuntimeException("DB down"));

            assertThatThrownBy(() -> orderService.createOrder(testOrder))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("DB down");

            verify(orderEventProducer, never()).publishOrderEvent(any());
        }
    }

    @Nested
    @DisplayName("validatePayment")
    class ValidatePaymentTests {

        @Test
        @DisplayName("should transition PENDING order to PAYMENT_VALIDATED")
        void validatePayment_pendingOrder_succeeds() {
            when(orderRepository.findByOrderId("order-001")).thenReturn(Optional.of(testOrder));
            when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            Order result = orderService.validatePayment("order-001");

            assertThat(result.getStatus()).isEqualTo(OrderStatus.PAYMENT_VALIDATED);
            verify(orderEventProducer).publishOrderEvent(
                    argThat(e -> "PAYMENT_VALIDATED".equals(e.getEventType()))
            );
            verify(orderMetrics).incrementPaymentValidated();
        }

        @Test
        @DisplayName("should throw when order not in PENDING state")
        void validatePayment_nonPendingOrder_throws() {
            testOrder.setStatus(OrderStatus.SHIPPED);
            when(orderRepository.findByOrderId("order-001")).thenReturn(Optional.of(testOrder));

            assertThatThrownBy(() -> orderService.validatePayment("order-001"))
                    .isInstanceOf(OrderProcessingException.class)
                    .hasMessageContaining("not in PENDING state");
        }

        @Test
        @DisplayName("should throw OrderNotFoundException for unknown orderId")
        void validatePayment_unknownOrder_throws() {
            when(orderRepository.findByOrderId("bad-id")).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orderService.validatePayment("bad-id"))
                    .isInstanceOf(OrderNotFoundException.class)
                    .hasMessageContaining("bad-id");
        }
    }

    @Nested
    @DisplayName("handleFailure")
    class HandleFailureTests {

        @Test
        @DisplayName("should mark order DEAD_LETTERED after 3 retries")
        void handleFailure_maxRetries_deadLetters() {
            testOrder.setRetryCount(2); // one more will exceed threshold
            when(orderRepository.findByOrderId("order-001")).thenReturn(Optional.of(testOrder));
            when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            Order result = orderService.handleFailure("order-001", "Timeout");

            assertThat(result.getStatus()).isEqualTo(OrderStatus.DEAD_LETTERED);
            assertThat(result.getRetryCount()).isEqualTo(3);
            verify(orderMetrics).incrementDeadLettered();
        }

        @Test
        @DisplayName("should mark order FAILED and increment retry count below threshold")
        void handleFailure_belowMaxRetries_marksFailedWithRetry() {
            testOrder.setRetryCount(0);
            when(orderRepository.findByOrderId("order-001")).thenReturn(Optional.of(testOrder));
            when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            Order result = orderService.handleFailure("order-001", "Network error");

            assertThat(result.getStatus()).isEqualTo(OrderStatus.FAILED);
            assertThat(result.getRetryCount()).isEqualTo(1);
            assertThat(result.getFailureReason()).isEqualTo("Network error");
            verify(orderMetrics).incrementOrderFailed();
        }
    }

    @Nested
    @DisplayName("shipOrder")
    class ShipOrderTests {

        @Test
        @DisplayName("should ship INVENTORY_RESERVED order successfully")
        void shipOrder_success() {
            testOrder.setStatus(OrderStatus.INVENTORY_RESERVED);
            when(orderRepository.findByOrderId("order-001")).thenReturn(Optional.of(testOrder));
            when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            Order result = orderService.shipOrder("order-001");

            assertThat(result.getStatus()).isEqualTo(OrderStatus.SHIPPED);
            verify(orderEventProducer).publishOrderEvent(
                    argThat(e -> "ORDER_SHIPPED".equals(e.getEventType()))
            );
        }

        @Test
        @DisplayName("should reject shipping if inventory not reserved")
        void shipOrder_inventoryNotReserved_throws() {
            testOrder.setStatus(OrderStatus.PAYMENT_VALIDATED);
            when(orderRepository.findByOrderId("order-001")).thenReturn(Optional.of(testOrder));

            assertThatThrownBy(() -> orderService.shipOrder("order-001"))
                    .isInstanceOf(OrderProcessingException.class)
                    .hasMessageContaining("inventory not reserved");
        }
    }
}
