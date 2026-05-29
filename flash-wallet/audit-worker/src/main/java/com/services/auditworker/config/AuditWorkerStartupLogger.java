package com.services.auditworker.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AuditWorkerStartupLogger {

    private final AuditWorkerProperties properties;
    private final KafkaProperties kafkaProperties;

    @EventListener(ApplicationReadyEvent.class)
    public void logStartupConfiguration() {
        log.info(
                "Audit worker configuration loaded: topic={}, dltTopic={}, consumerGroup={}, bootstrapServers={}, concurrency={}, maxRetries={}, backoffInitialMs={}, backoffMultiplier={}, backoffMaxMs={}",
                properties.getTopic(),
                properties.getDltTopic(),
                kafkaProperties.getConsumer().getGroupId(),
                kafkaProperties.getBootstrapServers(),
                properties.getListenerConcurrency(),
                properties.getRetry().getMaxRetries(),
                properties.getRetry().getInitialIntervalMs(),
                properties.getRetry().getMultiplier(),
                properties.getRetry().getMaxIntervalMs());
    }
}
