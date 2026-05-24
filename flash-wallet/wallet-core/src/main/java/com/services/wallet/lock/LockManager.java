package com.services.wallet.lock;

import com.services.wallet.exception.LockAcquisitionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class LockManager {

    private final RedissonClient redissonClient;

    private static final String LOCK_PREFIX = "lock:wallet:";

    /**
     * Executes a callback within the context of a single wallet lock.
     */
    public <T> T executeWithLock(UUID walletId, long waitTimeMs, long leaseTimeMs, LockCallback<T> callback) throws Exception {
        String lockKey = LOCK_PREFIX + walletId.toString();
        RLock lock = redissonClient.getLock(lockKey);
        
        log.debug("Attempting to acquire single lock for: {}", lockKey);
        boolean acquired = false;
        try {
            acquired = lock.tryLock(waitTimeMs, leaseTimeMs, TimeUnit.MILLISECONDS);
            if (!acquired) {
                log.warn("Failed to acquire lock for wallet: {}", lockKey);
                throw new LockAcquisitionException("Could not acquire lock for wallet: " + walletId);
            }
            log.debug("Successfully acquired lock for: {}", lockKey);
            return callback.doInLock();
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.debug("Released lock for: {}", lockKey);
            }
        }
    }

    /**
     * Executes a callback holding two wallet locks.
     * Enforces alphanumeric sorted lock ordering to avoid deadlocks.
     */
    public <T> T executeWithDoubleLocks(UUID walletIdA, UUID walletIdB, long waitTimeMs, long leaseTimeMs, LockCallback<T> callback) throws Exception {
        if (walletIdA.equals(walletIdB)) {
            throw new IllegalArgumentException("Cannot lock the same wallet twice: " + walletIdA);
        }

        // Sort UUIDs to enforce deterministic lock ordering
        String lockKey1;
        String lockKey2;
        UUID firstId;
        UUID secondId;

        if (walletIdA.toString().compareTo(walletIdB.toString()) < 0) {
            firstId = walletIdA;
            secondId = walletIdB;
        } else {
            firstId = walletIdB;
            secondId = walletIdA;
        }

        lockKey1 = LOCK_PREFIX + firstId.toString();
        lockKey2 = LOCK_PREFIX + secondId.toString();

        RLock lock1 = redissonClient.getLock(lockKey1);
        RLock lock2 = redissonClient.getLock(lockKey2);

        log.debug("Attempting to acquire double locks in order: {} -> {}", lockKey1, lockKey2);
        
        boolean acquiredLock1 = false;
        boolean acquiredLock2 = false;
        try {
            // Acquire first lock
            acquiredLock1 = lock1.tryLock(waitTimeMs, leaseTimeMs, TimeUnit.MILLISECONDS);
            if (!acquiredLock1) {
                log.warn("Failed to acquire lock 1: {}", lockKey1);
                throw new LockAcquisitionException("Could not acquire lock for wallet: " + firstId);
            }
            log.debug("Acquired lock 1: {}", lockKey1);

            // Acquire second lock
            acquiredLock2 = lock2.tryLock(waitTimeMs, leaseTimeMs, TimeUnit.MILLISECONDS);
            if (!acquiredLock2) {
                log.warn("Failed to acquire lock 2: {}", lockKey2);
                throw new LockAcquisitionException("Could not acquire lock for wallet: " + secondId);
            }
            log.debug("Acquired lock 2: {}", lockKey2);

            return callback.doInLock();
        } finally {
            // Release in reverse order of acquisition (not strictly required by Redis, but best practice)
            if (acquiredLock2 && lock2.isHeldByCurrentThread()) {
                lock2.unlock();
                log.debug("Released lock 2: {}", lockKey2);
            }
            if (acquiredLock1 && lock1.isHeldByCurrentThread()) {
                lock1.unlock();
                log.debug("Released lock 1: {}", lockKey1);
            }
        }
    }
}
