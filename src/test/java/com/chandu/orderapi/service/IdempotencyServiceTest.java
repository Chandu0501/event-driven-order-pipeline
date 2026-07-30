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
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    @DisplayName("tryClaim returns true on first call (set-if-absent succeeds)")
    void tryClaim_firstTime_returnsTrue() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(true);
        assertThat(idempotencyService.tryClaim("evt-003")).isTrue();
        verify(valueOperations).setIfAbsent(eq("order:processed:evt-003"), eq("1"), eq(Duration.ofHours(24)));
    }

    @Test
    @DisplayName("tryClaim returns false when another consumer owns the claim")
    void tryClaim_raceCondition_returnsFalse() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(false);
        assertThat(idempotencyService.tryClaim("evt-004")).isFalse();
    }

    @Test
    @DisplayName("releaseClaim deletes the Redis key")
    void releaseClaim_deletesKey() {
        idempotencyService.releaseClaim("evt-005");
        verify(redisTemplate).delete("order:processed:evt-005");
    }
}
