package com.chandu.orderapi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderEvent {

    private String eventId;
    private String eventType;        // ORDER_CREATED, PAYMENT_VALIDATED, etc.
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal totalAmount;
    private OrderStatus status;
    private String failureReason;
    private Instant eventTimestamp;
    private int retryCount;
    private String correlationId;    // For distributed tracing
    private String sourceService;

    public static OrderEvent from(Order order, String eventType, String sourceService) {
        return OrderEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(eventType)
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .totalAmount(order.getTotalAmount())
                .status(order.getStatus())
                .failureReason(order.getFailureReason())
                .retryCount(order.getRetryCount())
                .eventTimestamp(Instant.now())
                .sourceService(sourceService)
                .build();
    }
}
