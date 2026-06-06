package com.services.apigateway.controller;

import com.services.apigateway.exception.GatewayErrorResponse;
import com.services.apigateway.filter.CorrelationIdFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
public class FallbackController {

    @RequestMapping(value = "/fallback/wallet-core", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<GatewayErrorResponse> walletCoreFallback(ServerWebExchange exchange) {
        String traceId = resolveTraceId(exchange);
        String path = exchange.getRequest().getURI().getRawPath();

        exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
        exchange.getResponse().getHeaders().set(CorrelationIdFilter.HEADER_NAME, traceId);

        return Mono.just(GatewayErrorResponse.of(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Service Unavailable",
                "The wallet-core service is currently unavailable. The circuit breaker is open. Please retry later.",
                path,
                traceId));
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object value = exchange.getAttribute(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE);
        if (value instanceof String traceId && !traceId.isBlank()) {
            return traceId;
        }
        return UUID.randomUUID().toString();
    }
}
