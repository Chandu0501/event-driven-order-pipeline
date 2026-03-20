package com.chandu.orderapi.repository;

import com.chandu.orderapi.domain.Order;
import com.chandu.orderapi.domain.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<Order, String> {

    Optional<Order> findByOrderId(String orderId);

    List<Order> findByCustomerId(String customerId);

    List<Order> findByStatus(OrderStatus status);

    @Query("SELECT o FROM Order o WHERE o.status = :status AND o.createdAt < :threshold")
    List<Order> findStaleOrders(@Param("status") OrderStatus status, @Param("threshold") Instant threshold);

    @Modifying
    @Query("UPDATE Order o SET o.status = :status, o.failureReason = :reason WHERE o.orderId = :orderId")
    int updateOrderStatus(@Param("orderId") String orderId,
                          @Param("status") OrderStatus status,
                          @Param("reason") String reason);

    @Query("SELECT COUNT(o) FROM Order o WHERE o.status = :status")
    long countByStatus(@Param("status") OrderStatus status);

    @Query("SELECT o FROM Order o WHERE o.retryCount < :maxRetries AND o.status = 'FAILED'")
    List<Order> findRetryableOrders(@Param("maxRetries") int maxRetries);
}
