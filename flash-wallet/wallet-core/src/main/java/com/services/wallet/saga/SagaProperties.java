package com.services.wallet.saga;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Typed configuration for the Transfer Saga consumer.
 * Bound from the {@code wallet.saga} prefix in application.yml.
 */
@Data
@Component
@ConfigurationProperties(prefix = "wallet.saga")
public class SagaProperties {

    /** Kafka topic for internal saga coordination events. */
    private String topic;

    /** Dead letter topic for saga events that exhaust all retries. */
    private String dltTopic;

    /** Number of concurrent Kafka listener threads for saga events. */
    private int listenerConcurrency = 1;

    private Retry retry = new Retry();

    @Data
    public static class Retry {
        /** Maximum number of retry attempts before sending to DLT. */
        private int maxRetries = 3;

        /** Initial backoff interval in milliseconds. */
        private long initialIntervalMs = 1000;

        /** Backoff multiplier applied on each subsequent retry. */
        private double multiplier = 2.0;

        /** Maximum backoff interval in milliseconds. */
        private long maxIntervalMs = 4000;
    }
}
