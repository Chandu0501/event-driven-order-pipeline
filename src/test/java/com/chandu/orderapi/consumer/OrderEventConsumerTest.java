package com.chandu.orderapi.consumer;

import com.chandu.orderapi.domain.OrderEvent;
import com.chandu.orderapi.domain.OrderStatus;
import com.chandu.orderapi.metrics.OrderMetrics;
import com.chandu.orderapi.service.IdempotencyService;
import com.chandu.orderapi.service.OrderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderEventConsumer Unit Tests")
class OrderEventConsumerTest {

    @Mock private OrderService orderService;
    @Mock private IdempotencyService idempotencyService;
    @Mock private OrderMetrics orderMetrics;
    @Mock private Acknowledgment acknowledgment;

    @InjectMocks
    private OrderEventConsumer consumer;

    private OrderEvent testEvent;

    @BeforeEach
    void setUp() {
        testEvent = OrderEvent.builder()
                .eventId("evt-001")
                .eventType("ORDER_CREATED")
                .orderId("order-001")
                .customerId("cust-123")
                .productId("prod-456")
                .quantity(1)
                .totalAmount(new BigDecimal("49.99"))
                .status(OrderStatus.PENDING)
                .retryCount(0)
                .eventTimestamp(Instant.now())
                .build();
    }

    private ConsumerRecord<String, OrderEvent> record(OrderEvent event) {
        return new ConsumerRecord<>("order.events.v1", 0, 0L, event.getOrderId(), event);
    }

    @Test
    @DisplayName("should process ORDER_CREATED event and ACK")
    void consume_orderCreated_triggersPaymentValidation() {
        when(idempotencyService.isDuplicate("evt-001")).thenReturn(false);
        when(idempotencyService.markProcessed("evt-001")).thenReturn(true);

        consumer.consumeOrderEvent(record(testEvent), acknowledgment);

        verify(orderService).validatePayment("order-001");
        verify(idempotencyService).markProcessed("evt-001");
        verify(acknowledgment).acknowledge();
        verify(orderMetrics).incrementEventProcessed("ORDER_CREATED");
    }

    @Test
    @DisplayName("should skip duplicate event and ACK without processing")
    void consume_duplicateEvent_skipsAndAcks() {
        when(idempotencyService.isDuplicate("evt-001")).thenReturn(true);

        consumer.consumeOrderEvent(record(testEvent), acknowledgment);

        verify(orderService, never()).validatePayment(any());
        verify(acknowledgment).acknowledge();
        verify(orderMetrics).incrementDuplicateSkipped();
    }

    @Test
    @DisplayName("should NOT ACK on processing failure (triggers retry)")
    void consume_processingException_doesNotAck() {
        when(idempotencyService.isDuplicate("evt-001")).thenReturn(false);
        doThrow(new RuntimeException("DB timeout")).when(orderService).validatePayment("order-001");

        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> consumer.consumeOrderEvent(record(testEvent), acknowledgment));

        verify(acknowledgment, never()).acknowledge();
        verify(orderMetrics).incrementProcessingError();
    }

    @Test
    @DisplayName("should process PAYMENT_VALIDATED event by triggering inventory reservation")
    void consume_paymentValidated_triggersInventoryReservation() {
        testEvent.setEventType("PAYMENT_VALIDATED");
        testEvent.setStatus(OrderStatus.PAYMENT_VALIDATED);
        when(idempotencyService.isDuplicate("evt-001")).thenReturn(false);
        when(idempotencyService.markProcessed("evt-001")).thenReturn(true);

        consumer.consumeOrderEvent(record(testEvent), acknowledgment);

        verify(orderService).reserveInventory("order-001");
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("should process INVENTORY_RESERVED event by triggering shipment")
    void consume_inventoryReserved_triggersShipment() {
        testEvent.setEventType("INVENTORY_RESERVED");
        testEvent.setStatus(OrderStatus.INVENTORY_RESERVED);
        when(idempotencyService.isDuplicate("evt-001")).thenReturn(false);
        when(idempotencyService.markProcessed("evt-001")).thenReturn(true);

        consumer.consumeOrderEvent(record(testEvent), acknowledgment);

        verify(orderService).shipOrder("order-001");
        verify(acknowledgment).acknowledge();
    }
}
