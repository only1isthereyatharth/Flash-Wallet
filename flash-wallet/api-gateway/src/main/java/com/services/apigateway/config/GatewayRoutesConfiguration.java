package com.services.apigateway.config;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class GatewayRoutesConfiguration {

    private final ApiGatewayProperties properties;

    @Bean
    public RouteLocator gatewayRoutes(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("wallet-core-route", route -> route
                        .path("/api/v1/wallets/**")
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
