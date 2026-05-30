package com.services.wallet.exception;

import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ErrorResponse {
        private Instant timestamp;
        private int status;
        private String error;
        private String message;
        private String path;
    }

    @ExceptionHandler(WalletNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleWalletNotFound(WalletNotFoundException ex, HttpServletRequest request) {
        log.warn("Wallet not found exception: {}", ex.getMessage());
        return buildResponse(HttpStatus.NOT_FOUND, "Wallet Not Found", ex.getMessage(), request);
    }

    @ExceptionHandler(InsufficientBalanceException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientBalance(InsufficientBalanceException ex, HttpServletRequest request) {
        log.warn("Insufficient balance exception: {}", ex.getMessage());
        return buildResponse(HttpStatus.UNPROCESSABLE_ENTITY, "Insufficient Balance", ex.getMessage(), request);
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyConflict(IdempotencyConflictException ex, HttpServletRequest request) {
        log.warn("Idempotency conflict: {}", ex.getMessage());
        return buildResponse(HttpStatus.CONFLICT, "Idempotency Conflict", ex.getMessage(), request);
    }

    @ExceptionHandler(IdempotencyValidationException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyValidation(IdempotencyValidationException ex, HttpServletRequest request) {
        log.warn("Idempotency validation failed: {}", ex.getMessage());
        return buildResponse(HttpStatus.BAD_REQUEST, "Bad Request", ex.getMessage(), request);
    }

    @ExceptionHandler(LockAcquisitionException.class)
    public ResponseEntity<ErrorResponse> handleLockAcquisition(LockAcquisitionException ex, HttpServletRequest request) {
        log.error("Redisson Lock Acquisition failed: {}", ex.getMessage());
        return buildResponse(HttpStatus.CONFLICT, "Lock Acquisition Timeout", "Could not acquire distributed transaction locks. Please retry.", request);
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    public ResponseEntity<ErrorResponse> handleOptimisticLocking(ObjectOptimisticLockingFailureException ex, HttpServletRequest request) {
        log.error("JPA Optimistic Lock check failed (secondary defense level): {}", ex.getMessage());
        return buildResponse(HttpStatus.CONFLICT, "Concurrent Update Conflict", "This resource was modified concurrently. Please retry.", request);
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(DataIntegrityViolationException ex, HttpServletRequest request) {
        String rootMessage = ex.getMostSpecificCause().getMessage();
        if (rootMessage != null && rootMessage.contains("idempotency_key")) {
            log.warn("Duplicate idempotency key detected via DB constraint: {}", rootMessage);
            return buildResponse(HttpStatus.CONFLICT, "Idempotency Conflict",
                    "A transaction with this Idempotency-Key already exists.", request);
        }
        if (rootMessage != null && rootMessage.contains("user_id")) {
            log.warn("Duplicate wallet for user detected via DB constraint: {}", rootMessage);
            return buildResponse(HttpStatus.CONFLICT, "Duplicate Wallet",
                    "A wallet already exists for this user.", request);
        }
        log.error("Data integrity violation: {}", rootMessage, ex);
        return buildResponse(HttpStatus.CONFLICT, "Data Conflict",
                "A data integrity constraint was violated. Please check your request.", request);
    }

    @ExceptionHandler(IdempotencyPayloadMismatchException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyPayloadMismatch(IdempotencyPayloadMismatchException ex, HttpServletRequest request) {
        log.warn("Idempotency payload mismatch: {}", ex.getMessage());
        return buildResponse(HttpStatus.UNPROCESSABLE_ENTITY, "Idempotency Payload Mismatch", ex.getMessage(), request);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
        log.warn("Illegal argument exception: {}", ex.getMessage());
        return buildResponse(HttpStatus.BAD_REQUEST, "Bad Request", ex.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String validationErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));
        log.warn("Validation failure: {}", validationErrors);
        return buildResponse(HttpStatus.BAD_REQUEST, "Validation Failed", validationErrors, request);
    }

    @ExceptionHandler(org.springframework.http.converter.HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(org.springframework.http.converter.HttpMessageNotReadableException ex, HttpServletRequest request) {
        log.warn("HTTP message not readable (JSON parse error): {}", ex.getMessage());
        return buildResponse(HttpStatus.BAD_REQUEST, "Malformed JSON", "The request body is malformed or contains invalid data formats (e.g. invalid UUID).", request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception caught by global boundary: {}", ex.getMessage(), ex);
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error", "An unexpected system error occurred.", request);
    }

    private ResponseEntity<ErrorResponse> buildResponse(HttpStatus status, String error, String message, HttpServletRequest request) {
        ErrorResponse response = ErrorResponse.builder()
                .timestamp(Instant.now())
                .status(status.value())
                .error(error)
                .message(message)
                .path(request.getRequestURI())
                .build();
        return ResponseEntity.status(status).body(response);
    }
}
