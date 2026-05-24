package com.services.wallet.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Value("${swagger.server-url:http://localhost:8080}")
    private String serverUrl;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Flash-Wallet Ledger Core API")
                        .version("1.0")
                        .description("High-throughput digital wallet API supporting atomic idempotency, distributed lock ordering, and transaction logs."))
                .servers(List.of(
                        new Server().url(serverUrl).description("API Gateway (Port 8080)"),
                        new Server().url("http://localhost:8081").description("Direct Wallet Core (Port 8081)")
                ));
    }
}
