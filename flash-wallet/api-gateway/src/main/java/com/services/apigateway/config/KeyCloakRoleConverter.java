package com.services.apigateway.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

public class KeyCloakRoleConverter implements Converter<Jwt, Collection<GrantedAuthority>>{

    @SuppressWarnings("unchecked")
    @Override
    public Collection<GrantedAuthority> convert(Jwt source) {
        List<GrantedAuthority> authorities = new ArrayList<>();

        Map<String, Object> realmAccess = (Map<String, Object>) source.getClaim("realm_access");
        if(realmAccess == null || realmAccess.isEmpty()){
            return new ArrayList<>();
        }

        ((List<String>) realmAccess.get("roles"))
        .stream()
        .map(roleName -> "ROLE_" + roleName)
        .map(SimpleGrantedAuthority::new)
        .forEach(authorities::add);
        
        String scopeClaim = source.getClaimAsString("scope");
        if (scopeClaim != null && !scopeClaim.isBlank()) {
            Arrays.stream(scopeClaim.split(" "))
            .map(String::trim)
            .filter(p -> !p.isBlank())
            .map(p -> new SimpleGrantedAuthority("SCOPE_" + p))
            .forEach(authorities::add);
        }
        return authorities;
    }
}
