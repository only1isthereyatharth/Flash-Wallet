package com.services.apigateway.filter;

import com.services.apigateway.config.ApiGatewayProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * WebFilter (not GlobalFilter) so that security headers are injected at the
 * Spring WebFlux layer — wrapping gateway filters, circuit-breaker fallbacks,
 * and error handlers alike.
 */
@Component
@RequiredArgsConstructor
public class SecurityHeadersFilter implements WebFilter, Ordered {

    private final ApiGatewayProperties properties;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        exchange.getResponse().beforeCommit(() -> {
            addSecurityHeaders(exchange);
            return Mono.empty();
        });
        return chain.filter(exchange);
    }

    private void addSecurityHeaders(ServerWebExchange exchange) {
        HttpHeaders headers = exchange.getResponse().getHeaders();

        headers.set("X-Content-Type-Options", "nosniff");
        headers.set("X-Frame-Options", "DENY");
        headers.set("Referrer-Policy", "no-referrer");

        String path = exchange.getRequest().getURI().getRawPath();
        if (path.startsWith("/api/v1/wallets/")) {
            headers.set("Cache-Control", "no-store");
        }

        if (properties.getSecurity().isHstsEnabled()) {
            headers.set("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE; // First in WebFilter chain (runs before all other WebFilters)
    }
}
