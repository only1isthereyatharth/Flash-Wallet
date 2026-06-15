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
        http.authorizeExchange(ex -> ex.pathMatchers("swagger-ui/**", "/v3/api-docs/***").permitAll()
        .pathMatchers("/actuator/**").hasRole("ADMIN")
        .pathMatchers(HttpMethod.GET, "/api/v1/audit/**").hasAnyRole("AUDITOR", "ADMIN")
        .pathMatchers(HttpMethod.GET, "/api/v1/audit/failures").hasRole("ADMIN")
        .pathMatchers(HttpMethod.POST, "/api/v1/wallets", "/api/v1/wallet/deposit", "/api/v1/wallets/transfer").hasAnyRole("CUSTOMER", "SERVICE_CLIENT")
        .pathMatchers(HttpMethod.GET, "/api/v1/wallets/**").hasAnyRole("CUSTOMER", "AUDITOR", "ADMIN")
        .anyExchange().authenticated());

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
