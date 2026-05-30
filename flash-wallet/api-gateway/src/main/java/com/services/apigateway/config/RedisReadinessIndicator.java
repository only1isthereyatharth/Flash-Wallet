package com.services.apigateway.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component("redisRateLimitStore")
@RequiredArgsConstructor
@Slf4j
public class RedisReadinessIndicator implements ReactiveHealthIndicator {

    private final ReactiveStringRedisTemplate redisTemplate;

    @Override
    public Mono<Health> health() {
        return redisTemplate.getConnectionFactory()
                .getReactiveConnection()
                .ping()
                .map(response -> Health.up()
                        .withDetail("response", response)
                        .build())
                .timeout(Duration.ofSeconds(3))
                .onErrorResume(ex -> {
                    log.warn("Redis readiness check failed: {}", ex.getMessage());
                    return Mono.just(Health.down()
                            .withDetail("error", ex.getMessage())
                            .build());
                });
    }
}
