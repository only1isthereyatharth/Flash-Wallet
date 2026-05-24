package com.services.wallet.lock;

@FunctionalInterface
public interface LockCallback<T> {
    T doInLock() throws Exception;
}
