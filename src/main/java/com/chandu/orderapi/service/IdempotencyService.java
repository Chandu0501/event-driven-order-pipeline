package com.chandu.orderapi.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Guarantees exactly-once processing of Kafka events.
 * Stores processed event IDs in Redis with a 24-hour TTL.
 * On consumer restart or rebalance, duplicate events are safely skipped.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final String IDEMPOTENCY_KEY_PREFIX = "order:processed:";
    private static final Duration EVENT_TTL = Duration.ofHours(24);

    private final RedisTemplate<String, String> redisTemplate;

    /**
     * Returns true if this event has already been processed (duplicate).
     */
    public boolean isDuplicate(String eventId) {
        String key = IDEMPOTENCY_KEY_PREFIX + eventId;
        Boolean exists = redisTemplate.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    /**
     * Marks an event as processed. Returns false if a race condition
     * caused another thread to mark it first (atomic set-if-absent).
     */
    public boolean markProcessed(String eventId) {
        String key = IDEMPOTENCY_KEY_PREFIX + eventId;
        Boolean set = redisTemplate.opsForValue().setIfAbsent(key, "1", EVENT_TTL);
        if (Boolean.FALSE.equals(set)) {
            log.warn("Idempotency conflict: eventId={} was already marked processed by another thread", eventId);
            return false;
        }
        log.debug("Marked event as processed: eventId={}", eventId);
        return true;
    }

    /**
     * Removes idempotency key — used when processing fails and we want to allow retry.
     */
    public void clearProcessed(String eventId) {
        String key = IDEMPOTENCY_KEY_PREFIX + eventId;
        redisTemplate.delete(key);
        log.debug("Cleared idempotency key for retry: eventId={}", eventId);
    }
}
