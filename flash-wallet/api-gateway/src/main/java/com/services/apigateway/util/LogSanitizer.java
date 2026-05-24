package com.services.apigateway.util;

import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Component
public class LogSanitizer {

    private static final Set<String> SENSITIVE_HEADERS = Set.of(
            HttpHeaders.AUTHORIZATION.toLowerCase(Locale.ROOT),
            HttpHeaders.PROXY_AUTHORIZATION.toLowerCase(Locale.ROOT),
            HttpHeaders.COOKIE.toLowerCase(Locale.ROOT),
            HttpHeaders.SET_COOKIE.toLowerCase(Locale.ROOT));

    public Map<String, String> sanitize(HttpHeaders headers, int maxValueLength) {
        Map<String, String> sanitized = new LinkedHashMap<>();
        headers.forEach((headerName, values) -> sanitized.put(headerName, sanitizeValue(headerName, values, maxValueLength)));
        return sanitized;
    }

    private String sanitizeValue(String headerName, Iterable<String> values, int maxValueLength) {
        if (SENSITIVE_HEADERS.contains(headerName.toLowerCase(Locale.ROOT))) {
            return "***";
        }

        String joinedValues = String.join(", ", values);
        if (joinedValues.length() <= maxValueLength) {
            return joinedValues;
        }
        return joinedValues.substring(0, maxValueLength) + "...";
    }
}
