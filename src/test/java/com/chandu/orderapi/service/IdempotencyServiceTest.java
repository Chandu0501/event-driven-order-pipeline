package com.chandu.orderapi.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("IdempotencyService Unit Tests")
class IdempotencyServiceTest {

    @Mock private RedisTemplate<String, String> redisTemplate;
    @Mock private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private IdempotencyService idempotencyService;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    @DisplayName("isDuplicate returns true when key exists in Redis")
    void isDuplicate_keyExists_returnsTrue() {
        when(redisTemplate.hasKey("order:processed:evt-001")).thenReturn(true);
        assertThat(idempotencyService.isDuplicate("evt-001")).isTrue();
    }

    @Test
    @DisplayName("isDuplicate returns false when key absent")
    void isDuplicate_keyAbsent_returnsFalse() {
        when(redisTemplate.hasKey("order:processed:evt-002")).thenReturn(false);
        assertThat(idempotencyService.isDuplicate("evt-002")).isFalse();
    }

    @Test
    @DisplayName("markProcessed returns true on first call (set-if-absent succeeds)")
    void markProcessed_firstTime_returnsTrue() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(true);
        assertThat(idempotencyService.markProcessed("evt-003")).isTrue();
        verify(valueOperations).setIfAbsent(eq("order:processed:evt-003"), eq("1"), eq(Duration.ofHours(24)));
    }

    @Test
    @DisplayName("markProcessed returns false on race condition (set-if-absent fails)")
    void markProcessed_raceCondition_returnsFalse() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(false);
        assertThat(idempotencyService.markProcessed("evt-004")).isFalse();
    }

    @Test
    @DisplayName("clearProcessed deletes the Redis key")
    void clearProcessed_deletesKey() {
        idempotencyService.clearProcessed("evt-005");
        verify(redisTemplate).delete("order:processed:evt-005");
    }
}
