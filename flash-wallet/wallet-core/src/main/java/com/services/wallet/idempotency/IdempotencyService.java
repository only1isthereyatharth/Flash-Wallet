package com.services.wallet.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final RedissonClient redissonClient;
    private static final String KEY_PREFIX = "idempotency:";

    /**
     * Attempts atomic registration of a request key under PROCESSING status.
     * Uses setIfAbsent (SETNX equivalent) with a guard TTL (e.g. 5 minutes) to avoid dead state if a crash happens mid-flight.
     */
    public boolean tryStart(String key, Duration processingTimeout) {
        RBucket<IdempotencyState> bucket = redissonClient.getBucket(KEY_PREFIX + key);
        IdempotencyState state = new IdempotencyState("PROCESSING", null, 0);
        boolean success = bucket.setIfAbsent(state, processingTimeout);
        log.debug("Idempotency tryStart for key: {}. Result: {}", key, success);
        return success;
    }

    /**
     * Retrieves the current idempotency state for a given key.
     */
    public IdempotencyState get(String key) {
        RBucket<IdempotencyState> bucket = redissonClient.getBucket(KEY_PREFIX + key);
        return bucket.get();
    }

    /**
     * Marks the idempotency key as COMPLETED and buffers the payload with a 24-hour TTL.
     */
    public void complete(String key, String responseBody, int statusCode) {
        RBucket<IdempotencyState> bucket = redissonClient.getBucket(KEY_PREFIX + key);
        IdempotencyState state = new IdempotencyState("COMPLETED", responseBody, statusCode);
        bucket.set(state, Duration.ofHours(24));
        log.debug("Idempotency complete for key: {}", key);
    }

    /**
     * Deletes the idempotency key in case of processing failure so it can be retried.
     */
    public void fail(String key) {
        RBucket<IdempotencyState> bucket = redissonClient.getBucket(KEY_PREFIX + key);
        boolean deleted = bucket.delete();
        log.warn("Idempotency failed/cleared key: {}. Deleted status: {}", key, deleted);
    }
}
