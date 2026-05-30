package com.services.apigateway.config;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;

@Configuration
@RequiredArgsConstructor
public class RateLimiterConfig {

    private final ApiGatewayProperties properties;

    @Bean
    @Primary
    public KeyResolver hybridKeyResolver() {
        return exchange -> {
            String path = exchange.getRequest().getURI().getPath();

            // 1. Strictly enforce IP-based rate limiting for future auth paths (login/register)
            if (path.startsWith("/api/v1/auth/")) {
                return Mono.justOrEmpty(exchange.getRequest().getRemoteAddress())
                        .map(addr -> "ip:" + addr.getAddress().getHostAddress())
                        .defaultIfEmpty("ip:anonymous");
            }

            // 2. Attempt to resolve by X-Client-Id or X-Client header for other routes
            String clientId = exchange.getRequest().getHeaders().getFirst("X-Client-Id");
            if (clientId == null || clientId.trim().isEmpty()) {
                clientId = exchange.getRequest().getHeaders().getFirst("X-Client");
            }

            if (clientId != null && !clientId.trim().isEmpty()) {
                return Mono.just("client:" + clientId.trim());
            }

            // 3. Fall back to IP address if header is not present
            return Mono.justOrEmpty(exchange.getRequest().getRemoteAddress())
                    .map(addr -> "ip:" + addr.getAddress().getHostAddress())
                    .defaultIfEmpty("ip:anonymous");
        };
    }

    @Bean
    public RedisRateLimiter redisRateLimiter() {
        return new RedisRateLimiter(
                properties.getRateLimit().getReplenishRate(),
                properties.getRateLimit().getBurstCapacity(),
                properties.getRateLimit().getRequestedTokens());
    }
}
