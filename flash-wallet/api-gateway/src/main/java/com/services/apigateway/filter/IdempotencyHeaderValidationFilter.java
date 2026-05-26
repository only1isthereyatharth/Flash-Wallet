package com.services.apigateway.filter;

import com.services.apigateway.config.ApiGatewayProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.PathContainer;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class IdempotencyHeaderValidationFilter implements GlobalFilter, Ordered {

    private static final List<HttpMethod> MUTATING_METHODS = List.of(
            HttpMethod.POST,
            HttpMethod.PUT,
            HttpMethod.PATCH,
            HttpMethod.DELETE);

    private final ApiGatewayProperties properties;
    private final PathPatternParser pathPatternParser = new PathPatternParser();

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        HttpMethod method = exchange.getRequest().getMethod();
        String path = exchange.getRequest().getURI().getRawPath();

        if (method == null || !MUTATING_METHODS.contains(method) || !requiresIdempotencyHeader(path)) {
            return chain.filter(exchange);
        }

        String traceId = Objects.toString(exchange.getAttribute(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE), "unknown");
        String idempotencyKey = exchange.getRequest().getHeaders().getFirst("Idempotency-Key");

        if (!StringUtils.hasText(idempotencyKey)) {
            log.warn("Gateway rejected mutable request without Idempotency-Key: traceId={}, method={}, path={}", traceId, method, path);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Required header 'Idempotency-Key' is missing or empty.");
        }

        if (properties.getIdempotency().isStrictUuid()) {
            try {
                UUID.fromString(idempotencyKey);
            } catch (IllegalArgumentException exception) {
                log.warn("Gateway rejected invalid Idempotency-Key format: traceId={}, method={}, path={}, idempotencyKey={}", traceId, method, path, idempotencyKey);
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Header 'Idempotency-Key' must be a valid UUID.");
            }
        }

        log.info("Gateway validated Idempotency-Key for mutable request: traceId={}, method={}, path={}, idempotencyKey={}", traceId, method, path, idempotencyKey);
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10; // Third in precedence
    }

    private boolean requiresIdempotencyHeader(String requestPath) {
        PathContainer pathContainer = PathContainer.parsePath(requestPath);
        return properties.getIdempotency().getRequiredPaths().stream()
                .map(pathPatternParser::parse)
                .anyMatch(pattern -> pattern.matches(pathContainer));
    }
}
