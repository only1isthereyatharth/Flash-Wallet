package com.services.apigateway.filter;

import java.util.stream.Collectors;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import reactor.core.publisher.Mono;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 4) // Runs Fifth in precedence
public final class UserContextPropagationFilter implements GlobalFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        return ReactiveSecurityContextHolder.getContext()
            .map(SecurityContext::getAuthentication)
            .cast(JwtAuthenticationToken.class)
            .flatMap(token -> {
                String userId = token.getToken().getSubject(); // JWT 'sub' claim = Keycloak user UUID
                String roles = token.getAuthorities().stream()
                    .map(GrantedAuthority::getAuthority)
                    .filter(a -> a.startsWith("ROLE_"))
                    .map(a -> a.substring(5))    // strip "ROLE_" prefix
                    .collect(Collectors.joining(","));
                String scopes = token.getAuthorities().stream()
                    .map(GrantedAuthority::getAuthority)
                    .filter(a -> a.startsWith("SCOPE_"))
                    .map(a -> a.substring(6))    // strip "SCOPE_" prefix
                    .collect(Collectors.joining(","));
                ServerWebExchange mutated = exchange.mutate()
                    .request(r -> r
                        .header("X-User-Id", userId)  // Add user id from JWT sub claim
                        .header("X-User-Roles", roles)  // Add user roles from realm_access
                        .header("X-User-Scopes", scopes) // Add user scopes from scope claim
                        .headers(h -> h.remove(HttpHeaders.AUTHORIZATION)) // Strip JWT from internal call
                    )
                    .build();
                return chain.filter(mutated);
            })
            .switchIfEmpty(chain.filter(exchange));
    }
}
