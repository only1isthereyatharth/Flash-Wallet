package com.services.wallet.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.servers.Server;

import org.springdoc.core.customizers.OperationCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.services.wallet.idempotency.Idempotent;

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

    @Bean
    public OperationCustomizer customizeIdempotencyHeader() {
        return (operation, handlerMethod) -> {
            if (handlerMethod.hasMethodAnnotation(Idempotent.class)) {
                if (operation.getParameters() == null) {
                    operation.setParameters(new java.util.ArrayList<>());
                }
                operation.addParametersItem(new Parameter()
                        .name("Idempotency-Key")
                        .in("header")
                        .required(true)
                        .description("Unique UUID key to ensure transaction idempotency")
                        .schema(new Schema<>().type("string").format("uuid")));
            }
            return operation;
        };
    }
}
