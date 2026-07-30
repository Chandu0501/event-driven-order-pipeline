package com.chandu.orderapi.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Custom Micrometer metrics exposed via /actuator/prometheus.
 * Exported through the Prometheus actuator endpoint for dashboards and alerts.
 */
@Component
public class OrderMetrics {

    private final Counter ordersCreated;
    private final Counter paymentsValidated;
    private final Counter inventoryReserved;
    private final Counter ordersShipped;
    private final Counter ordersFailed;
    private final Counter deadLettered;
    private final Counter duplicatesSkipped;
    private final Counter publishSuccess;
    private final Counter publishFailure;
    private final Counter eventsProcessed;
    private final Counter processingErrors;
    private final Counter circuitBreakerOpen;

    public OrderMetrics(MeterRegistry registry) {
        this.ordersCreated      = Counter.builder("orders.created").description("Total orders created").register(registry);
        this.paymentsValidated  = Counter.builder("orders.payment.validated").register(registry);
        this.inventoryReserved  = Counter.builder("orders.inventory.reserved").register(registry);
        this.ordersShipped      = Counter.builder("orders.shipped").register(registry);
        this.ordersFailed       = Counter.builder("orders.failed").register(registry);
        this.deadLettered       = Counter.builder("orders.dead_lettered").register(registry);
        this.duplicatesSkipped  = Counter.builder("orders.events.duplicate_skipped").register(registry);
        this.publishSuccess     = Counter.builder("kafka.publish.success").register(registry);
        this.publishFailure     = Counter.builder("kafka.publish.failure").register(registry);
        this.eventsProcessed    = Counter.builder("kafka.events.processed").register(registry);
        this.processingErrors   = Counter.builder("kafka.events.processing_error").register(registry);
        this.circuitBreakerOpen = Counter.builder("circuit_breaker.open").register(registry);
    }

    public void incrementOrderCreated()      { ordersCreated.increment(); }
    public void incrementPaymentValidated()   { paymentsValidated.increment(); }
    public void incrementInventoryReserved()  { inventoryReserved.increment(); }
    public void incrementOrderShipped()       { ordersShipped.increment(); }
    public void incrementOrderFailed()        { ordersFailed.increment(); }
    public void incrementDeadLettered()       { deadLettered.increment(); }
    public void incrementDuplicateSkipped()   { duplicatesSkipped.increment(); }
    public void incrementPublishSuccess()     { publishSuccess.increment(); }
    public void incrementPublishFailure()     { publishFailure.increment(); }
    public void incrementEventProcessed(String type) { eventsProcessed.increment(); }
    public void incrementProcessingError()    { processingErrors.increment(); }
    public void incrementCircuitBreakerOpen() { circuitBreakerOpen.increment(); }
}
