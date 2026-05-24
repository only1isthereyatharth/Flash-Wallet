package com.services.auditworker.exception;

public class AuditDeadLetterPublishException extends RuntimeException {

    public AuditDeadLetterPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
