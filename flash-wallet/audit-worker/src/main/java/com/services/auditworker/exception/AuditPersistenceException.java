package com.services.auditworker.exception;

public class AuditPersistenceException extends RuntimeException {

    public AuditPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
