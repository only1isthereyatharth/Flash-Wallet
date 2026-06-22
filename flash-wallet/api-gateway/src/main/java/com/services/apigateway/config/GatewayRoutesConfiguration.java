package com.services.apigateway.config;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.factory.RequestSizeGatewayFilterFactory;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.util.unit.DataSize;

@Configuration
@RequiredArgsConstructor
public class GatewayRoutesConfiguration {

    private final ApiGatewayProperties properties;
    private final RedisRateLimiter redisRateLimiter;
    private final KeyResolver hybridKeyResolver;
    private final RequestSizeGatewayFilterFactory requestSizeFilterFactory;

    @Bean
    public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("wallet-core-route", route -> route
                        .path("/api/v1/wallets/**")
                        .and().method(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.DELETE)
                        .filters(f -> {
                            f.filter(requestSizeFilterFactory.apply(c -> c.setMaxSize(DataSize.ofKilobytes(10))))
                                    .requestRateLimiter(r -> r
                                            .setRateLimiter(redisRateLimiter)
                                            .setKeyResolver(hybridKeyResolver));

                            // Circuit breaker on wallet-core route (3.1)
                            if (properties.getResilience().getCircuitBreaker().isEnabled()) {
                                f.circuitBreaker(cb -> cb
                                        .setName("walletCoreCircuitBreaker")
                                        .setFallbackUri("forward:/fallback/wallet-core"));
                            }

                            // Retry on GET only — max 2 retries on 5xx/connect errors with jittered backoff (3.2)
                            f.retry(retryConfig -> retryConfig
                                    .setRetries(2)
                                    .setMethods(HttpMethod.GET)
                                    .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY, HttpStatus.SERVICE_UNAVAILABLE, HttpStatus.GATEWAY_TIMEOUT)
                                    .setBackoff(java.time.Duration.ofMillis(100), java.time.Duration.ofMillis(1000), 2, true));

                            return f;
                        })
                        .uri(properties.getServices().getWalletCoreUri()))
                .route("wallet-core-swagger-ui", route -> route
                        .path("/swagger-ui/**")
                        .and().method(HttpMethod.GET)
                        .uri(properties.getServices().getWalletCoreUri()))
                .route("wallet-core-api-docs", route -> route
                        .path("/v3/api-docs/**")
                        .and().method(HttpMethod.GET)
                        .uri(properties.getServices().getWalletCoreUri()))
                // Route for audit-worker — GET-only (read audit logs, AUDITOR/ADMIN role enforced upstream)
                .route("audit-worker-api", r -> r
                        .path("/api/v1/audit/**")
                        .and().method(HttpMethod.GET)
                        .filters(f -> {
                            // Rate limiting — same Redis bucket strategy as wallet-core
                            f.requestRateLimiter(rl -> rl
                                    .setRateLimiter(redisRateLimiter)
                                    .setKeyResolver(hybridKeyResolver));

                            // Circuit breaker — isolated from wallet-core circuit breaker
                            if (properties.getResilience().getCircuitBreaker().isEnabled()) {
                                f.circuitBreaker(cb -> cb
                                        .setName("auditWorkerCircuitBreaker")
                                        .setFallbackUri("forward:/fallback/audit-worker"));
                            }

                            // Retry on GET — safe, all audit reads are idempotent
                            f.retry(retryConfig -> retryConfig
                                    .setRetries(2)
                                    .setMethods(HttpMethod.GET)
                                    .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY, HttpStatus.SERVICE_UNAVAILABLE, HttpStatus.GATEWAY_TIMEOUT)
                                    .setBackoff(java.time.Duration.ofMillis(100), java.time.Duration.ofMillis(1000), 2, true));

                            return f;
                        })
                        .uri(properties.getServices().getAuditWorkerUri()))
                .build();
    }
}
