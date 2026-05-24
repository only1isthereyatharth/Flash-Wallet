package com.services.auditworker.exception;

public class AuditEventDeserializationException extends RuntimeException {

    public AuditEventDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
