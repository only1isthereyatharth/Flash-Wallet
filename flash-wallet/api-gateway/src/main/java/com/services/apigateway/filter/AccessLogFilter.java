package com.services.apigateway.filter;

import com.services.apigateway.config.ApiGatewayProperties;
import com.services.apigateway.util.LogSanitizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
@Slf4j
public class AccessLogFilter implements GlobalFilter, Ordered {

    private final ApiGatewayProperties properties;
    private final LogSanitizer logSanitizer;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long startTimeNanos = System.nanoTime();
        String traceId = resolveTraceId(exchange);
        String method = Objects.toString(exchange.getRequest().getMethod(), "UNKNOWN");
        String path = exchange.getRequest().getURI().getRawPath();
        String query = exchange.getRequest().getURI().getRawQuery();
        String remoteAddress = resolveRemoteAddress(exchange);

        log.info(
                "Gateway request received: traceId={}, method={}, path={}, query={}, remoteAddress={}",
                traceId,
                method,
                path,
                query,
                remoteAddress);

        if (properties.getLogging().isLogHeaders()) {
            log.debug(
                    "Gateway request headers: traceId={}, headers={}",
                    traceId,
                    logSanitizer.sanitize(exchange.getRequest().getHeaders(), properties.getLogging().getMaxHeaderValueLength()));
        }

        AtomicReference<Throwable> failureReference = new AtomicReference<>();
        return chain.filter(exchange)
                .doOnError(failureReference::set)
                .doFinally(signalType -> logCompletion(exchange, traceId, startTimeNanos, failureReference.get()));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 2; // Third in precedence
    }

    private void logCompletion(ServerWebExchange exchange, String traceId, long startTimeNanos, Throwable failure) {
        long durationMs = (System.nanoTime() - startTimeNanos) / 1_000_000L;
        HttpStatusCode statusCode = exchange.getResponse().getStatusCode();
        int status = statusCode != null ? statusCode.value() : 500;
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        String routeId = route != null ? route.getId() : "unmatched";

        if (properties.getLogging().isLogHeaders()) {
            log.debug(
                    "Gateway response headers: traceId={}, headers={}",
                    traceId,
                    logSanitizer.sanitize(exchange.getResponse().getHeaders(), properties.getLogging().getMaxHeaderValueLength()));
        }

        if (failure != null) {
            log.error(
                    "Gateway request failed before completion: traceId={}, routeId={}, status={}, durationMs={}, exceptionType={}, message={}",
                    traceId,
                    routeId,
                    status,
                    durationMs,
                    failure.getClass().getSimpleName(),
                    failure.getMessage());
            return;
        }

        if (status >= 500) {
            log.error(
                    "Gateway response completed with server error: traceId={}, routeId={}, status={}, durationMs={}",
                    traceId,
                    routeId,
                    status,
                    durationMs);
            return;
        }

        if (status >= 400) {
            log.warn(
                    "Gateway response completed with client-visible error: traceId={}, routeId={}, status={}, durationMs={}",
                    traceId,
                    routeId,
                    status,
                    durationMs);
            return;
        }

        if (durationMs >= properties.getLogging().getSlowRequestThresholdMs()) {
            log.warn(
                    "Gateway response completed slowly: traceId={}, routeId={}, status={}, durationMs={}",
                    traceId,
                    routeId,
                    status,
                    durationMs);
            return;
        }

        log.info(
                "Gateway response completed successfully: traceId={}, routeId={}, status={}, durationMs={}",
                traceId,
                routeId,
                status,
                durationMs);
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object value = exchange.getAttribute(CorrelationIdFilter.REQUEST_ID_ATTRIBUTE);
        return value instanceof String traceId ? traceId : "unknown";
    }

    private String resolveRemoteAddress(ServerWebExchange exchange) {
        InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
        if (remoteAddress == null) {
            return "unknown";
        }
        return remoteAddress.getAddress() != null
                ? remoteAddress.getAddress().getHostAddress()
                : remoteAddress.getHostString();
    }
}
