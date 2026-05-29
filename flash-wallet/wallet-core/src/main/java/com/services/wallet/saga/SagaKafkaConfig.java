package com.services.wallet.saga;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.Map;

/**
 * Kafka listener infrastructure for the Transfer Saga consumer.
 *
 * <p>Uses a dedicated consumer group ({@code wallet-core-saga-group}) and a separate
 * {@link ConcurrentKafkaListenerContainerFactory} so it does not interfere with any
 * other Kafka configuration in {@code wallet-core}.
 *
 * <p>Retry policy mirrors the audit-worker approach: exponential back-off followed by
 * DLT routing via {@link SagaDltRecoverer} once all retries are exhausted.
 */
@Configuration
@Slf4j
public class SagaKafkaConfig {

    /**
     * Consumer factory for saga events.
     * Overrides {@code group.id} so this consumer is completely isolated from
     * any producer-side Kafka config defined in {@code application.yml}.
     */
    @Bean
    public ConsumerFactory<String, String> sagaConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put("group.id", "wallet-core-saga-group");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Producer factory for the Saga DLT publisher.
     * Uses String serialization because saga events are raw JSON strings on the wire.
     */
    @Bean
    public ProducerFactory<String, String> sagaProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * String-typed KafkaTemplate used exclusively by {@link SagaDltRecoverer}.
     * Named explicitly so it does not conflict with the auto-configured
     * {@code KafkaTemplate<String, TransactionEvent>} used by {@code WalletEventProducer}.
     */
    @Bean
    public KafkaTemplate<String, String> sagaStringKafkaTemplate(
            ProducerFactory<String, String> sagaProducerFactory) {
        return new KafkaTemplate<>(sagaProducerFactory);
    }

    /**
     * Error handler with exponential back-off and DLT routing, driven by
     * {@link SagaProperties} values from {@code application.yml}.
     */
    @Bean
    public DefaultErrorHandler sagaErrorHandler(
            SagaDltRecoverer sagaDltRecoverer,
            SagaProperties sagaProperties) {

        SagaProperties.Retry retry = sagaProperties.getRetry();

        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(retry.getMaxRetries());
        backOff.setInitialInterval(retry.getInitialIntervalMs());
        backOff.setMultiplier(retry.getMultiplier());
        backOff.setMaxInterval(retry.getMaxIntervalMs());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(sagaDltRecoverer, backOff);
        errorHandler.setCommitRecovered(true);
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn(
                        "Saga event retry scheduled: attempt={}, topic={}, partition={}, offset={}, key={}, exceptionType={}, message={}",
                        deliveryAttempt,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        ex.getClass().getSimpleName(),
                        ex.getMessage());
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error(
                        "Saga event DLT routing failed: topic={}, partition={}, offset={}, key={}, originalEx={}, recoveryEx={}",
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

    /**
     * Listener container factory used by {@link TransferSagaConsumer}.
     * {@code AckMode.RECORD} ensures offset is committed only after successful processing.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> sagaKafkaListenerContainerFactory(
            ConsumerFactory<String, String> sagaConsumerFactory,
            DefaultErrorHandler sagaErrorHandler,
            SagaProperties sagaProperties) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sagaConsumerFactory);
        factory.setCommonErrorHandler(sagaErrorHandler);
        factory.setConcurrency(sagaProperties.getListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
