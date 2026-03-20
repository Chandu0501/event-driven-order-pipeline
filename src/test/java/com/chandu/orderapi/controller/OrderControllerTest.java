package com.chandu.orderapi.controller;

import com.chandu.orderapi.domain.Order;
import com.chandu.orderapi.domain.OrderStatus;
import com.chandu.orderapi.exception.GlobalExceptionHandler;
import com.chandu.orderapi.exception.OrderNotFoundException;
import com.chandu.orderapi.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderController Unit Tests")
class OrderControllerTest {

    @Mock private OrderService orderService;

    @InjectMocks
    private OrderController controller;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;
    private Order sampleOrder;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        sampleOrder = Order.builder()
                .orderId("order-001")
                .customerId("cust-123")
                .productId("prod-456")
                .quantity(2)
                .totalAmount(new BigDecimal("79.98"))
                .status(OrderStatus.PENDING)
                .retryCount(0)
                .build();
    }

    @Test
    @DisplayName("POST /api/v1/orders - returns 201 with created order")
    void createOrder_returns201() throws Exception {
        when(orderService.createOrder(any())).thenReturn(sampleOrder);

        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleOrder)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value("order-001"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    @DisplayName("GET /api/v1/orders/{id} - returns 200 with order")
    void getOrder_found_returns200() throws Exception {
        when(orderService.getOrder("order-001")).thenReturn(sampleOrder);

        mockMvc.perform(get("/api/v1/orders/order-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("order-001"))
                .andExpect(jsonPath("$.customerId").value("cust-123"));
    }

    @Test
    @DisplayName("GET /api/v1/orders/{id} - returns 404 when not found")
    void getOrder_notFound_returns404() throws Exception {
        when(orderService.getOrder("bad-id")).thenThrow(new OrderNotFoundException("bad-id"));

        mockMvc.perform(get("/api/v1/orders/bad-id"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/v1/orders/customer/{id} - returns list of orders")
    void getOrdersByCustomer_returnsList() throws Exception {
        when(orderService.getOrdersByCustomer("cust-123")).thenReturn(List.of(sampleOrder));

        mockMvc.perform(get("/api/v1/orders/customer/cust-123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].customerId").value("cust-123"));
    }

    @Test
    @DisplayName("POST /api/v1/orders/{id}/validate-payment - returns validated order")
    void validatePayment_success() throws Exception {
        sampleOrder.setStatus(OrderStatus.PAYMENT_VALIDATED);
        when(orderService.validatePayment("order-001")).thenReturn(sampleOrder);

        mockMvc.perform(post("/api/v1/orders/order-001/validate-payment"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PAYMENT_VALIDATED"));
    }
}
