package com.chandu.orderapi.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Provides a bounded idempotency window for Kafka event processing.
 * Redis SET NX atomically lets one consumer claim an event ID for 24 hours.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final String IDEMPOTENCY_KEY_PREFIX = "order:processed:";
    private static final Duration EVENT_TTL = Duration.ofHours(24);

    private final RedisTemplate<String, String> redisTemplate;

    /**
     * Returns true only when this caller acquires the event claim.
     */
    public boolean tryClaim(String eventId) {
        String key = IDEMPOTENCY_KEY_PREFIX + eventId;
        Boolean set = redisTemplate.opsForValue().setIfAbsent(key, "1", EVENT_TTL);
        if (Boolean.FALSE.equals(set)) {
            log.warn("Idempotency conflict: eventId={} is already claimed", eventId);
            return false;
        }
        return Boolean.TRUE.equals(set);
    }

    /**
     * Removes idempotency key — used when processing fails and we want to allow retry.
     */
    public void releaseClaim(String eventId) {
        String key = IDEMPOTENCY_KEY_PREFIX + eventId;
        redisTemplate.delete(key);
        log.debug("Cleared idempotency key for retry: eventId={}", eventId);
    }
}
