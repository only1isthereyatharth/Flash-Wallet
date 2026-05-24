package com.services.apigateway.exception;

import org.springframework.http.HttpStatus;

import java.time.Instant;

public record GatewayErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        String traceId,
        String service) {

    public static GatewayErrorResponse of(HttpStatus status, String error, String message, String path, String traceId) {
        return new GatewayErrorResponse(
                Instant.now(),
                status.value(),
                error,
                message,
                path,
                traceId,
                "api-gateway");
    }
}
