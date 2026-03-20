package com.chandu.orderapi.domain;

public enum OrderStatus {
    PENDING,
    PROCESSING,
    PAYMENT_VALIDATED,
    INVENTORY_RESERVED,
    SHIPPED,
    DELIVERED,
    FAILED,
    DEAD_LETTERED
}
