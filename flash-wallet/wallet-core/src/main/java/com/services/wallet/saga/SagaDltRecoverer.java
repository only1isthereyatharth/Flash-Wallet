package com.services.wallet.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.wallet.event.TransactionEvent;
import com.services.wallet.model.Transaction;
import com.services.wallet.model.TransactionStatus;
import com.services.wallet.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Dead-letter recoverer for the Saga coordination topic.
 *
 * <p>When a saga event exhausts all retry attempts, this recoverer publishes
 * it to {@code wallet.saga.events.DLT} with diagnostic metadata headers.
 * Events landing in the DLT represent saga steps that could not complete
 * automatically and require manual operational intervention.
 *
 * <p>Uses the plain {@code String} Kafka template (same as audit-worker's
 * approach) because saga events are serialised as JSON strings on the wire.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaDltRecoverer implements ConsumerRecordRecoverer {

    private static final long DLT_PUBLISH_TIMEOUT_SECONDS = 10L;

    // Diagnostic headers added to every DLT record
    private static final String HEADER_ORIGINAL_TOPIC = "saga-original-topic";
    private static final String HEADER_ORIGINAL_PARTITION = "saga-original-partition";
    private static final String HEADER_ORIGINAL_OFFSET = "saga-original-offset";
    private static final String HEADER_EXCEPTION_CLASS = "saga-exception-class";
    private static final String HEADER_EXCEPTION_MESSAGE = "saga-exception-message";

    /**
     * Plain String KafkaTemplate — the saga topic carries JSON strings,
     * matching the consumer factory deserialisers in {@link SagaKafkaConfig}.
     * We reuse the auto-configured Spring Boot template (String, String).
     */
    private final KafkaTemplate<String, String> sagaStringKafkaTemplate;
    private final SagaProperties sagaProperties;
    private final TransactionRepository transactionRepository;
    private final ObjectMapper objectMapper;

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        log.error(
                "Terminal saga event failure; routing to DLT: " +
                        "topic={}, dltTopic={}, partition={}, offset={}, key={}, exceptionType={}, message={}",
                record.topic(),
                sagaProperties.getDltTopic(),
                record.partition(),
                record.offset(),
                record.key(),
                exception.getClass().getSimpleName(),
                resolveExceptionMessage(exception),
                exception);

        ProducerRecord<String, String> dltRecord = buildDltRecord(record, exception);

        SendResult<String, String> sendResult;
        try {
            sendResult = sagaStringKafkaTemplate.send(dltRecord)
                    .get(DLT_PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error(
                    "DLT publish interrupted for saga event: topic={}, dltTopic={}, partition={}, offset={}, key={}",
                    record.topic(), sagaProperties.getDltTopic(),
                    record.partition(), record.offset(), record.key(), ex);
            throw new SagaDltPublishException("Interrupted while publishing saga event to DLT", ex);
        } catch (ExecutionException | TimeoutException ex) {
            log.error(
                    "Failed to publish saga event to DLT: topic={}, dltTopic={}, partition={}, offset={}, key={}",
                    record.topic(), sagaProperties.getDltTopic(),
                    record.partition(), record.offset(), record.key(), ex);
            throw new SagaDltPublishException("Failed to publish saga event to DLT", ex);
        }

        log.error(
                "Saga event routed to DLT — MANUAL INTERVENTION REQUIRED: " +
                        "originalTopic={}, dltTopic={}, originalPartition={}, originalOffset={}, " +
                        "key={}, dltPartition={}, dltOffset={}",
                record.topic(),
                sagaProperties.getDltTopic(),
                record.partition(),
                record.offset(),
                record.key(),
                sendResult.getRecordMetadata().partition(),
                sendResult.getRecordMetadata().offset());

        // Mark the transaction as FAILED — catastrophic terminal state
        markTransactionFailed(record);
    }

    private ProducerRecord<String, String> buildDltRecord(ConsumerRecord<?, ?> record, Exception exception) {
        RecordHeaders headers = new RecordHeaders();
        record.headers().forEach(headers::add);
        headers.add(HEADER_ORIGINAL_TOPIC, toBytes(record.topic()));
        headers.add(HEADER_ORIGINAL_PARTITION, toBytes(String.valueOf(record.partition())));
        headers.add(HEADER_ORIGINAL_OFFSET, toBytes(String.valueOf(record.offset())));
        headers.add(HEADER_EXCEPTION_CLASS, toBytes(exception.getClass().getName()));
        headers.add(HEADER_EXCEPTION_MESSAGE, toBytes(resolveExceptionMessage(exception)));

        return new ProducerRecord<>(
                sagaProperties.getDltTopic(),
                record.partition(),
                record.timestamp(),
                record.key() == null ? null : record.key().toString(),
                record.value() == null ? null : record.value().toString(),
                headers);
    }

    private String resolveExceptionMessage(Exception exception) {
        Throwable cause = NestedExceptionUtils.getMostSpecificCause(exception);
        if (cause == null || cause.getMessage() == null) {
            return exception.getMessage();
        }
        return cause.getMessage();
    }

    private byte[] toBytes(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Best-effort attempt to mark the transaction as {@code FAILED} after DLT routing.
     * This is a catastrophic terminal state: money is debited but not returned.
     * If this fails (e.g., DB is down), the DLT record still exists for manual recovery.
     */
    @Transactional
    protected void markTransactionFailed(ConsumerRecord<?, ?> record) {
        try {
            if (record.value() == null) {
                log.warn("Cannot mark transaction FAILED — DLT record has null value");
                return;
            }
            TransactionEvent event = objectMapper.readValue(
                    record.value().toString(), TransactionEvent.class);
            Optional<Transaction> optTx = transactionRepository.findById(event.transactionId());
            if (optTx.isEmpty()) {
                log.error("CRITICAL — Transaction not found for DLT event, manual reconciliation required: transactionId={}",
                        event.transactionId());
                return;
            }
            Transaction tx = optTx.get();
            if (tx.getStatus() == TransactionStatus.COMPENSATED
                || tx.getStatus() == TransactionStatus.COMPLETED) {
                log.info("Transaction already in terminal state {}, skipping FAILED mark: transactionId={}",
                        tx.getStatus(), event.transactionId());
                return;
            }
            tx.setStatus(TransactionStatus.FAILED);
            transactionRepository.save(tx);
            log.error("CRITICAL — Transaction marked FAILED, manual reconciliation required: transactionId={}",
                    event.transactionId());
        } catch (Exception ex) {
            log.error("Failed to mark transaction as FAILED after DLT routing. " +
                    "DLT record exists for manual recovery. record.key={}", record.key(), ex);
        }
    }
}
