package com.services.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;

import com.services.apigateway.config.KeyCloakRoleConverter;

import reactor.core.publisher.Mono;

@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {
    
    @Bean
    public SecurityWebFilterChain springSecurityWebFilterChain(ServerHttpSecurity http) {
        http.authorizeExchange(ex -> ex
                // 1. Public Documentation & Health Endpoints
                .pathMatchers("/swagger-ui/**", "/v3/api-docs/**", "/swagger-resources/**").permitAll()
                .pathMatchers("/actuator/health/**").permitAll() // Public K8s liveness/readiness
                
                // 2. Sensitive Actuator Endpoints (Metrics/Prometheus) — ADMIN only
                .pathMatchers("/actuator/**").hasRole("ADMIN")

                // 3. System Auditing (flash-audit-worker tracking) — ADMIN only
                .pathMatchers(HttpMethod.GET, "/api/v1/audit/failures").hasRole("ADMIN")
                
                // 4. Compliance Logs — AUDITOR + ADMIN
                .pathMatchers(HttpMethod.GET, "/api/v1/audit/**").hasAnyRole("ADMIN", "AUDITOR")
                
                // 5. NOTE: 'POST /api/v1/wallets' is REMOVED because provisioning is now async via Kafka!
                
                // 6. Wallet Financial Operations — CUSTOMER + SERVICE_CLIENT
                .pathMatchers(HttpMethod.POST, "/api/v1/wallets/deposit").hasAnyRole("CUSTOMER", "SERVICE_CLIENT")
                .pathMatchers(HttpMethod.POST, "/api/v1/wallets/transfer").hasAnyRole("CUSTOMER", "SERVICE_CLIENT")
                
                // 7. Transaction Status Polling — All Roles allowed at perimeter level
                // (Must come BEFORE general /wallets/** path rules to prevent shadowed matching)
                .pathMatchers(HttpMethod.GET, "/api/v1/wallets/transactions/**")
                    .hasAnyRole("CUSTOMER", "SERVICE_CLIENT", "ADMIN", "AUDITOR")
                
                // 8. General Wallet Queries — CUSTOMER + SERVICE_CLIENT + ADMIN + AUDITOR
                // (Added AUDITOR here based on our updated scope discussion so they can execute read operations)
                .pathMatchers(HttpMethod.GET, "/api/v1/wallets/**")
                    .hasAnyRole("CUSTOMER", "SERVICE_CLIENT", "ADMIN", "AUDITOR")
                
                // 9. Catch-all security net
                .anyExchange().authenticated()
            );

        http.csrf(ex -> ex.disable()); // currently no browser client
        http.oauth2ResourceServer(oauthResourceServer -> oauthResourceServer.jwt(jwt ->
            jwt.jwtAuthenticationConverter(jwtAuthenticationConverter())));

        return http.build();
        
    }

    private Converter<Jwt, Mono<AbstractAuthenticationToken>> jwtAuthenticationConverter() {
        JwtAuthenticationConverter jwtAuthenticationConverter = new JwtAuthenticationConverter();
        jwtAuthenticationConverter.setJwtGrantedAuthoritiesConverter(new KeyCloakRoleConverter());

        return new ReactiveJwtAuthenticationConverterAdapter(jwtAuthenticationConverter);
    }
}
