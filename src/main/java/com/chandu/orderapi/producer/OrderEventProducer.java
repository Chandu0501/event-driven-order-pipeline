package com.chandu.orderapi.producer;

import com.chandu.orderapi.domain.OrderEvent;
import com.chandu.orderapi.metrics.OrderMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    @Value("${kafka.topics.order-events}")
    private String orderEventsTopic;

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final OrderMetrics orderMetrics;

    /**
     * Publishes an OrderEvent to Kafka.
     * Uses orderId as the partition key to guarantee ordering per order.
     * Async send with callback for observability.
     */
    public void publishOrderEvent(OrderEvent event) {
        log.info("Publishing event: type={} orderId={} eventId={}",
                event.getEventType(), event.getOrderId(), event.getEventId());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(orderEventsTopic, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event: type={} orderId={} error={}",
                        event.getEventType(), event.getOrderId(), ex.getMessage(), ex);
                orderMetrics.incrementPublishFailure();
            } else {
                log.info("Event published successfully: type={} orderId={} partition={} offset={}",
                        event.getEventType(),
                        event.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                orderMetrics.incrementPublishSuccess();
            }
        });
    }
}
