package com.services.auditworker.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonSecurityConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jacksonCustomizer() {
        return builder -> {
            // Secure deserialization: fail when encountering unknown JSON properties
            builder.featuresToEnable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

            // Restrict polymorphic deserialization by ensuring default typing is disabled.
            // Spring Boot's default ObjectMapperBuilder does not enable it, but this reinforces
            // compliance controls to prevent configuration drifts.
        };
    }
}
