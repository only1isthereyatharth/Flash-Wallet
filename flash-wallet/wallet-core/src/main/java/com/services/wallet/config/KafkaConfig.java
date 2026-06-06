package com.services.wallet.config;

import com.services.wallet.event.TransactionEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaConfig {

    public static final String TRANSACTION_EVENTS_TOPIC = "wallet.transaction.events";
    public static final String SAGA_EVENTS_TOPIC = "wallet.saga.events";
    public static final String SAGA_DLT_TOPIC = "wallet.saga.events.DLT";

    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name(TRANSACTION_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1) // Single replica is suitable for our single-broker local Docker setup
                .build();
    }

    @Bean
    public NewTopic sagaEventsTopic() {
        return TopicBuilder.name(SAGA_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic sagaDltTopic() {
        return TopicBuilder.name(SAGA_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public ProducerFactory<String, TransactionEvent> walletProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(
                props,
                new StringSerializer(),
                new JsonSerializer<TransactionEvent>()
        );
    }

    @Bean
    public KafkaTemplate<String, TransactionEvent> kafkaTemplate(ProducerFactory<String,
        TransactionEvent> walletProducerFactory) {
        return new KafkaTemplate<>(walletProducerFactory);
    }
}
