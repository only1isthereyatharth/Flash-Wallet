package com.services.apigateway.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class GatewayStartupLogger {

    private final ApiGatewayProperties properties;
    private final RouteLocator routeLocator;

    @EventListener(ApplicationReadyEvent.class)
    public void logStartupConfiguration() {
        List<String> routeIds = routeLocator.getRoutes()
                .map(Route::getId)
                .collectList()
                .block(Duration.ofSeconds(5));

        log.info(
                "API gateway configuration loaded: routes={}, walletCoreUri={}, connectTimeoutMs={}, responseTimeoutMs={}, allowedOrigins={}, strictIdempotencyUuid={}, idempotencyProtectedPaths={}",
                routeIds,
                properties.getServices().getWalletCoreUri(),
                properties.getHttpClient().getConnectTimeoutMs(),
                properties.getHttpClient().getResponseTimeoutMs(),
                properties.getCors().getAllowedOrigins(),
                properties.getIdempotency().isStrictUuid(),
                properties.getIdempotency().getRequiredPaths());
    }
}
