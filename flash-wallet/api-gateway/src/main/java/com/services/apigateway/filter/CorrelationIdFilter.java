package com.services.apigateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

@Component
public class CorrelationIdFilter implements GlobalFilter, Ordered {

    public static final String HEADER_NAME = "X-Request-Id";
    public static final String REQUEST_ID_ATTRIBUTE = "gatewayRequestId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String requestId = Optional.ofNullable(exchange.getRequest().getHeaders().getFirst(HEADER_NAME))
                .filter(StringUtils::hasText)
                .orElseGet(() -> UUID.randomUUID().toString());

        exchange.getAttributes().put(REQUEST_ID_ATTRIBUTE, requestId);

        ServerHttpRequest request = exchange.getRequest()
                .mutate()
                .headers(headers -> headers.set(HEADER_NAME, requestId))
                .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
                .request(request)
                .build();
        mutatedExchange.getResponse().getHeaders().set(HEADER_NAME, requestId);

        return chain.filter(mutatedExchange);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE; // Will take the highest precedence in the filter chain
    }
}
