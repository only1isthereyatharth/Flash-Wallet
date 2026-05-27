package com.services.wallet.producer;

import com.services.wallet.config.KafkaConfig;
import com.services.wallet.event.TransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class WalletEventProducer {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    /**
     * Publishes a TransactionEvent payload asynchronously to Kafka.
     * Uses transaction ID as partition key to ensure order sequence preservation.
     */
    public void sendTransactionEvent(TransactionEvent event) {
        String key = event.transactionId().toString();
        log.info("Publishing event to Kafka: ID={}, EventType={}, Status={}", key, event.eventType(), event.status());

        CompletableFuture<SendResult<String, TransactionEvent>> future = 
                kafkaTemplate.send(KafkaConfig.TRANSACTION_EVENTS_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Kafka publishing failure for key: {}", key, ex);
            } else {
                log.info("Kafka publishing success: key={}, topic={}, partition={}, offset={}", 
                        key,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
