package com.services.auditworker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "audit.worker")
@Data
public class AuditWorkerProperties {

    private String topic = "wallet.transaction.events";
    private String dltTopic = "wallet.transaction.events.DLT";
    private int listenerConcurrency = 3;
    private Retry retry = new Retry();

    @Data
    public static class Retry {
        private int maxRetries = 3;
        private long initialIntervalMs = 1000L;
        private double multiplier = 2.0D;
        private long maxIntervalMs = 4000L;
    }
}
