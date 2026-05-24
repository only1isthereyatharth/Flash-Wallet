package com.services;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
@ConfigurationPropertiesScan(basePackages = "com.services.auditworker")
public class AuditWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditWorkerApplication.class, args);
    }
}
