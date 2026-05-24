package com.services.apigateway.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.apigateway.filter.CorrelationIdFilter;
import io.netty.handler.timeout.ReadTimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.codec.DecodingException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.server.MethodNotAllowedException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
@Slf4j
public class GatewayExceptionHandler implements ErrorWebExceptionHandler {

    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            return Mono.error(ex);
        }

        HttpStatus status = resolveStatus(ex);
        String traceId = resolveTraceId(exchange);
        String path = exchange.getRequest().getURI().getRawPath();
        String message = resolveMessage(status, ex);
        String error = resolveErrorLabel(status);

        GatewayErrorResponse errorResponse = GatewayErrorResponse.of(status, error, message, path, traceId);
        logFailure(exchange, traceId, status, ex);

        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().set(CorrelationIdFilter.HEADER_NAME, traceId);

        byte[] body = serialize(errorResponse);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }

    private HttpStatus resolveStatus(Throwable ex) {
        if (ex instanceof ResponseStatusException responseStatusException) {
            return HttpStatus.valueOf(responseStatusException.getStatusCode().value());
        }
        if (ex instanceof ErrorResponseException errorResponseException) {
            return HttpStatus.valueOf(errorResponseException.getStatusCode().value());
        }
        if (ex instanceof MethodNotAllowedException) {
            return HttpStatus.METHOD_NOT_ALLOWED;
        }
        if (ex instanceof ServerWebInputException || ex instanceof DecodingException || ex instanceof IllegalArgumentException) {
            return HttpStatus.BAD_REQUEST;
        }
        if (hasCause(ex, TimeoutException.class) || hasCause(ex, ReadTimeoutException.class)) {
            return HttpStatus.GATEWAY_TIMEOUT;
        }
        if (hasCause(ex, ConnectException.class)
                || hasCause(ex, UnknownHostException.class)
                || hasCause(ex, NoRouteToHostException.class)
                || hasCause(ex, UnresolvedAddressException.class)) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }

    private String resolveMessage(HttpStatus status, Throwable ex) {
        if (ex instanceof ResponseStatusException responseStatusException
                && responseStatusException.getReason() != null
                && !responseStatusException.getReason().isBlank()) {
            return responseStatusException.getReason();
        }

        return switch (status) {
            case BAD_REQUEST -> "The gateway rejected the request before forwarding it downstream.";
            case METHOD_NOT_ALLOWED -> "The requested HTTP method is not supported for this endpoint.";
            case SERVICE_UNAVAILABLE -> "The downstream service is unavailable right now. Please retry shortly.";
            case GATEWAY_TIMEOUT -> "The downstream service did not respond before the gateway timeout expired.";
            case NOT_FOUND -> "No gateway route matched the requested resource.";
            default -> "An unexpected gateway error occurred.";
        };
    }

    private String resolveErrorLabel(HttpStatus status) {
        return switch (status) {
            case BAD_REQUEST -> "Bad Request";
            case NOT_FOUND -> "Route Not Found";
            case METHOD_NOT_ALLOWED -> "Method Not Allowed";
            case SERVICE_UNAVAILABLE -> "Downstream Service Unavailable";
            case GATEWAY_TIMEOUT -> "Gateway Timeout";
            case BAD_GATEWAY -> "Bad Gateway";
            default -> "Internal Server Error";
        };
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
        Throwable current = throwable;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object currentTraceId = exchange.getAttribute(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE);
        if (currentTraceId instanceof String traceId && !traceId.isBlank()) {
            return traceId;
        }

        String generatedTraceId = UUID.randomUUID().toString();
        exchange.getAttributes().put(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE, generatedTraceId);
        return generatedTraceId;
    }

    private void logFailure(ServerWebExchange exchange, String traceId, HttpStatus status, Throwable ex) {
        String method = Objects.toString(exchange.getRequest().getMethod(), "UNKNOWN");
        String path = exchange.getRequest().getURI().getRawPath();

        if (status.is5xxServerError()) {
            log.error(
                    "Gateway exception handled: traceId={}, method={}, path={}, status={}, exceptionType={}, message={}",
                    traceId,
                    method,
                    path,
                    status.value(),
                    ex.getClass().getSimpleName(),
                    ex.getMessage(),
                    ex);
            return;
        }

        log.warn(
                "Gateway exception handled: traceId={}, method={}, path={}, status={}, exceptionType={}, message={}",
                traceId,
                method,
                path,
                status.value(),
                ex.getClass().getSimpleName(),
                ex.getMessage());
    }

    private byte[] serialize(GatewayErrorResponse errorResponse) {
        try {
            return objectMapper.writeValueAsBytes(errorResponse);
        } catch (JsonProcessingException jsonProcessingException) {
            log.error("Failed to serialize gateway error response", jsonProcessingException);
            String fallbackBody = "{\"status\":500,\"error\":\"Internal Server Error\",\"message\":\"Gateway error serialization failed.\"}";
            return fallbackBody.getBytes(StandardCharsets.UTF_8);
        }
    }
}
