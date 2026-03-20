package com.chandu.orderapi.consumer;

import com.chandu.orderapi.domain.OrderEvent;
import com.chandu.orderapi.metrics.OrderMetrics;
import com.chandu.orderapi.service.IdempotencyService;
import com.chandu.orderapi.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;
    private final IdempotencyService idempotencyService;
    private final OrderMetrics orderMetrics;

    /**
     * Main order event consumer.
     * Guarded by idempotency check — safe on consumer rebalance / at-least-once delivery.
     * Manual ACK after successful processing to prevent message loss.
     */
    @KafkaListener(
            topics = "${kafka.topics.order-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent event = record.value();

        log.info("Received event: type={} orderId={} eventId={} partition={} offset={}",
                event.getEventType(), event.getOrderId(), event.getEventId(),
                record.partition(), record.offset());

        // Idempotency guard — skip already-processed events
        if (idempotencyService.isDuplicate(event.getEventId())) {
            log.warn("Duplicate event skipped: eventId={} orderId={}", event.getEventId(), event.getOrderId());
            orderMetrics.incrementDuplicateSkipped();
            ack.acknowledge();
            return;
        }

        try {
            processEvent(event);
            idempotencyService.markProcessed(event.getEventId());
            orderMetrics.incrementEventProcessed(event.getEventType());
            ack.acknowledge();

            log.info("Event processed and ACKed: eventId={} type={}", event.getEventId(), event.getEventType());

        } catch (Exception ex) {
            log.error("Failed to process event: eventId={} type={} error={}",
                    event.getEventId(), event.getEventType(), ex.getMessage(), ex);
            orderMetrics.incrementProcessingError();
            // Do NOT ack — let the DefaultErrorHandler retry and eventually DLQ
            throw ex;
        }
    }

    /**
     * DLT (Dead Letter Topic) consumer — alerts on unrecoverable events.
     */
    @KafkaListener(
            topics = "${kafka.topics.order-events}.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void consumeDeadLetterEvent(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        log.error("DEAD LETTER EVENT: type={} orderId={} retryCount={}",
                event.getEventType(), event.getOrderId(), event.getRetryCount());

        orderMetrics.incrementDeadLettered();

        try {
            orderService.handleFailure(event.getOrderId(),
                    "Exhausted retries — routed to DLT after " + event.getRetryCount() + " attempts");
        } catch (Exception ex) {
            log.error("Failed to mark dead-lettered order: orderId={}", event.getOrderId(), ex);
        }
    }

    private void processEvent(OrderEvent event) {
        switch (event.getEventType()) {
            case "ORDER_CREATED"        -> orderService.validatePayment(event.getOrderId());
            case "PAYMENT_VALIDATED"    -> orderService.reserveInventory(event.getOrderId());
            case "INVENTORY_RESERVED"   -> orderService.shipOrder(event.getOrderId());
            default -> log.info("No handler for event type: {}", event.getEventType());
        }
    }
}
