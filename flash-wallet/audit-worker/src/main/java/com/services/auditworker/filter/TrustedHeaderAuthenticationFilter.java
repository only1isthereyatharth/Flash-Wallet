package com.services.auditworker.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public final class TrustedHeaderAuthenticationFilter extends OncePerRequestFilter{

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String userId = request.getHeader("X-User-Id");
        String userRoles = request.getHeader("X-User-Roles");
        String userScope = request.getHeader("X-User-Scopes");

        if(userId != null && userRoles != null && userScope != null) {
            List<GrantedAuthority> authorities = new ArrayList<>();
            Arrays.stream(userRoles.split(","))
                                            .map(String::trim)
                                            .filter(r -> !r.isBlank())
                                            .map(r -> new SimpleGrantedAuthority("ROLE_" + r))
                                            .forEach(authorities::add);
            
            Arrays.stream(userScope.split(","))
                                            .map(String::trim)
                                            .filter(r -> !r.isBlank())
                                            .map(r -> new SimpleGrantedAuthority("SCOPE_" + r))
                                            .forEach(authorities::add);

            UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken(userId, null, authorities);
            SecurityContextHolder.getContext().setAuthentication(auth);
        }
        filterChain.doFilter(request, response);
    }
    
}
