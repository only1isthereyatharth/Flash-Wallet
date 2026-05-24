package com.services.apigateway.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "flash.gateway")
@Validated
@Data
public class ApiGatewayProperties {

    private Services services = new Services();
    private HttpClient httpClient = new HttpClient();
    private Cors cors = new Cors();
    private Logging logging = new Logging();
    private Idempotency idempotency = new Idempotency();

    @Data
    public static class Services {
        @NotBlank
        private String walletCoreUri = "http://localhost:8081";
    }

    @Data
    public static class HttpClient {
        @Min(100)
        private int connectTimeoutMs = 3000;

        @Min(100)
        private long responseTimeoutMs = 10000;
    }

    @Data
    public static class Cors {
        @NotEmpty
        private List<String> allowedOrigins = new ArrayList<>(List.of("http://localhost:5173"));
        @NotEmpty
        private List<String> allowedMethods = new ArrayList<>(List.of("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"));
        @NotEmpty
        private List<String> allowedHeaders = new ArrayList<>(List.of("*"));
        @NotEmpty
        private List<String> exposedHeaders = new ArrayList<>(List.of("X-Request-Id"));
    }

    @Data
    public static class Logging {
        private boolean logHeaders = false;

        @Min(1)
        private int slowRequestThresholdMs = 2000;

        @Min(16)
        private int maxHeaderValueLength = 120;
    }

    @Data
    public static class Idempotency {
        private boolean strictUuid = true;
        private List<String> requiredPaths = new ArrayList<>(List.of(
                "/api/v1/wallets/transfer",
                "/api/v1/wallets/deposit"));
    }
}
