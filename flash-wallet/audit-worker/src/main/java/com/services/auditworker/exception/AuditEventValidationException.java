package com.services.auditworker.exception;

public class AuditEventValidationException extends RuntimeException {

    public AuditEventValidationException(String message) {
        super(message);
    }
}
