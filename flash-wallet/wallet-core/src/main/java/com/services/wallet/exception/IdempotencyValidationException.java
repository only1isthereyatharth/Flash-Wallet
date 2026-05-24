package com.services.wallet.exception;

public class IdempotencyValidationException extends RuntimeException {
    public IdempotencyValidationException(String message) {
        super(message);
    }
}
