package com.services.apigateway.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.apigateway.exception.GatewayErrorResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotFoundResponseWebFilter implements WebFilter, Ordered {

    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        return chain.filter(exchange)
                .then(Mono.defer(() -> writeGatewayErrorResponseIfNeeded(exchange)));
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    private Mono<Void> writeGatewayErrorResponseIfNeeded(ServerWebExchange exchange) {
        HttpStatusCode statusCode = exchange.getResponse().getStatusCode();
        if (statusCode == null || exchange.getResponse().isCommitted()) {
            return Mono.empty();
        }

        HttpStatus status = HttpStatus.resolve(statusCode.value());
        if (status != HttpStatus.NOT_FOUND && status != HttpStatus.METHOD_NOT_ALLOWED) {
            return Mono.empty();
        }

        String traceId = resolveTraceId(exchange);
        String method = Objects.toString(exchange.getRequest().getMethod(), "UNKNOWN");
        String path = exchange.getRequest().getURI().getRawPath();

        String errorLabel = status == HttpStatus.NOT_FOUND ? "Route Not Found" : "Method Not Allowed";
        String message = status == HttpStatus.NOT_FOUND
                ? "No gateway route matched the requested resource."
                : "The requested HTTP method is not supported for this endpoint.";

        GatewayErrorResponse responseBody = GatewayErrorResponse.of(status, errorLabel, message, path, traceId);
        log.warn("Gateway produced terminal {} response without downstream handler: traceId={}, method={}, path={}", status.value(), traceId, method, path);

        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        exchange.getResponse().getHeaders().set(CorrelationIdFilter.HEADER_NAME, traceId);
        return exchange.getResponse().writeWith(Mono.just(
                exchange.getResponse().bufferFactory().wrap(serialize(responseBody))));
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object value = exchange.getAttribute(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE);
        if (value instanceof String traceId && !traceId.isBlank()) {
            return traceId;
        }

        String generatedTraceId = UUID.randomUUID().toString();
        exchange.getAttributes().put(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE, generatedTraceId);
        return generatedTraceId;
    }

    private byte[] serialize(GatewayErrorResponse errorResponse) {
        try {
            return objectMapper.writeValueAsBytes(errorResponse);
        } catch (JsonProcessingException exception) {
            log.error("Failed to serialize terminal gateway error response", exception);
            String fallbackBody = "{\"status\":500,\"error\":\"Internal Server Error\",\"message\":\"Gateway error serialization failed.\"}";
            return fallbackBody.getBytes(StandardCharsets.UTF_8);
        }
    }
}
