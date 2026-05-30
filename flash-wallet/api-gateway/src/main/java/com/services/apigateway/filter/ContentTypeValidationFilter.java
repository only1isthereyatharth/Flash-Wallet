package com.services.apigateway.filter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

@Component
@Slf4j
public class ContentTypeValidationFilter implements GlobalFilter, Ordered {

    private static final List<HttpMethod> MUTATING_METHODS = List.of(
            HttpMethod.POST,
            HttpMethod.PUT,
            HttpMethod.PATCH);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        HttpMethod method = exchange.getRequest().getMethod();

        if (method == null || !MUTATING_METHODS.contains(method)) {
            return chain.filter(exchange);
        }

        MediaType contentType = exchange.getRequest().getHeaders().getContentType();
        if (contentType == null || !contentType.isCompatibleWith(MediaType.APPLICATION_JSON)) {
            String traceId = Objects.toString(
                    exchange.getAttribute(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE), "unknown");
            String path = exchange.getRequest().getURI().getRawPath();
            log.warn("Gateway rejected request with unsupported Content-Type: traceId={}, method={}, path={}, contentType={}",
                    traceId, method, path, contentType);
            throw new ResponseStatusException(HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                    "Content-Type must be application/json for " + method + " requests.");
        }

        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 8; // Before IdempotencyHeaderValidationFilter (+10)
    }
}
