package com.services.apigateway.config;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.factory.RequestSizeGatewayFilterFactory;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
                        .filters(f -> f
                                .filter(requestSizeFilterFactory.apply(c -> c.setMaxSize(DataSize.ofKilobytes(10))))
                                .requestRateLimiter(r -> r
                                        .setRateLimiter(redisRateLimiter)
                                        .setKeyResolver(hybridKeyResolver)))
                        .uri(properties.getServices().getWalletCoreUri()))
                .route("wallet-core-swagger-ui", route -> route
                        .path("/swagger-ui/**")
                        .uri(properties.getServices().getWalletCoreUri()))
                .route("wallet-core-api-docs", route -> route
                        .path("/v3/api-docs/**")
                        .uri(properties.getServices().getWalletCoreUri()))
                .build();
    }
}
