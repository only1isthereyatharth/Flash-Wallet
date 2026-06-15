package com.services.auditworker.config;

import com.services.auditworker.exception.AuditEventDeserializationException;
import com.services.auditworker.exception.AuditEventValidationException;
import com.services.auditworker.service.AuditDeadLetterRecoverer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
@Slf4j
public class AuditKafkaConfiguration {

    private static final int TOPIC_PARTITIONS = 3;

    @Bean
    public NewTopic transactionEventsTopic(AuditWorkerProperties properties) {
        return TopicBuilder.name(properties.getTopic())
                .partitions(TOPIC_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transactionEventsDeadLetterTopic(AuditWorkerProperties properties) {
        return TopicBuilder.name(properties.getDltTopic())
                .partitions(TOPIC_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    public ConsumerFactory<String, String> auditConsumerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

    @Bean
    public ProducerFactory<String, String> auditProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, String> auditKafkaTemplate(ProducerFactory<String, String> auditProducerFactory) {
        return new KafkaTemplate<>(auditProducerFactory);
    }

    @Bean
    public DefaultErrorHandler auditErrorHandler(
            AuditDeadLetterRecoverer auditDeadLetterRecoverer,
            AuditWorkerProperties properties) {
        AuditWorkerProperties.Retry retryProperties = properties.getRetry();

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(
                retryProperties.getMaxRetries());
        backOff.setInitialInterval(retryProperties.getInitialIntervalMs());
        backOff.setMultiplier(retryProperties.getMultiplier());
        backOff.setMaxInterval(retryProperties.getMaxIntervalMs());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(auditDeadLetterRecoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                AuditEventDeserializationException.class,
                AuditEventValidationException.class);
        errorHandler.setCommitRecovered(true);
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record,
                    Exception ex,
                    int deliveryAttempt) {
                log.warn(
                        "Kafka retry attempt scheduled for audit event: attempt={}, topic={}, partition={}, offset={}, key={}, exceptionType={}, message={}",
                        deliveryAttempt,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        ex.getClass().getSimpleName(),
                        ex.getMessage());
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record,
                    Exception original,
                    Exception failure) {
                log.error(
                        "Kafka recovery failed for audit event: topic={}, partition={}, offset={}, key={}, originalExceptionType={}, recoveryExceptionType={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        original.getClass().getSimpleName(),
                        failure.getClass().getSimpleName(),
                        failure);
            }
        });
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> auditKafkaListenerContainerFactory(
            ConsumerFactory<String, String> auditConsumerFactory,
            DefaultErrorHandler auditErrorHandler,
            AuditWorkerProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(auditConsumerFactory);
        factory.setCommonErrorHandler(auditErrorHandler);
        factory.setConcurrency(properties.getListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
