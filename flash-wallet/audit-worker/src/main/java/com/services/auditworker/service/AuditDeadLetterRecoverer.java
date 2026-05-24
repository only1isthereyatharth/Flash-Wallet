package com.services.auditworker.service;

import com.services.auditworker.config.AuditWorkerProperties;
import com.services.auditworker.exception.AuditDeadLetterPublishException;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@RequiredArgsConstructor
@Slf4j
public class AuditDeadLetterRecoverer implements ConsumerRecordRecoverer {

    private static final long DLT_PUBLISH_TIMEOUT_SECONDS = 10L;
    private static final String HEADER_ORIGINAL_TOPIC = "audit-original-topic";
    private static final String HEADER_ORIGINAL_PARTITION = "audit-original-partition";
    private static final String HEADER_ORIGINAL_OFFSET = "audit-original-offset";
    private static final String HEADER_EXCEPTION_CLASS = "audit-exception-class";
    private static final String HEADER_EXCEPTION_MESSAGE = "audit-exception-message";

    private final KafkaTemplate<String, String> auditKafkaTemplate;
    private final AuditFailureService auditFailureService;
    private final AuditWorkerProperties properties;

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        log.error(
                "Terminal audit event failure; routing to DLT: topic={}, dltTopic={}, partition={}, offset={}, key={}, exceptionType={}, message={}",
                record.topic(),
                properties.getDltTopic(),
                record.partition(),
                record.offset(),
                record.key(),
                exception.getClass().getSimpleName(),
                resolveExceptionMessage(exception),
                exception);

        ProducerRecord<String, String> deadLetterRecord = buildDeadLetterRecord(record, exception);

        SendResult<String, String> sendResult;
        try {
            sendResult = auditKafkaTemplate.send(deadLetterRecord)
                    .get(DLT_PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error(
                    "DLT publish interrupted for audit event: topic={}, dltTopic={}, partition={}, offset={}, key={}",
                    record.topic(),
                    properties.getDltTopic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    ex);
            throw new AuditDeadLetterPublishException(
                    "Interrupted while publishing audit event to dead-letter topic", ex);
        } catch (ExecutionException | TimeoutException ex) {
            log.error(
                    "Failed to publish audit event to DLT: topic={}, dltTopic={}, partition={}, offset={}, key={}",
                    record.topic(),
                    properties.getDltTopic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    ex);
            throw new AuditDeadLetterPublishException(
                    "Failed to publish audit event to dead-letter topic", ex);
        }

        log.error(
                "Audit event routed to DLT after retry exhaustion: originalTopic={}, dltTopic={}, originalPartition={}, originalOffset={}, key={}, dltPartition={}, dltOffset={}",
                record.topic(),
                properties.getDltTopic(),
                record.partition(),
                record.offset(),
                record.key(),
                sendResult.getRecordMetadata().partition(),
                sendResult.getRecordMetadata().offset());

        auditFailureService.recordRecoveredFailure(record, exception, properties.getDltTopic());
    }

    private ProducerRecord<String, String> buildDeadLetterRecord(ConsumerRecord<?, ?> record, Exception exception) {
        RecordHeaders headers = new RecordHeaders();
        record.headers().forEach(headers::add);
        headers.add(HEADER_ORIGINAL_TOPIC, toBytes(record.topic()));
        headers.add(HEADER_ORIGINAL_PARTITION, toBytes(String.valueOf(record.partition())));
        headers.add(HEADER_ORIGINAL_OFFSET, toBytes(String.valueOf(record.offset())));
        headers.add(HEADER_EXCEPTION_CLASS, toBytes(exception.getClass().getName()));
        headers.add(HEADER_EXCEPTION_MESSAGE, toBytes(resolveExceptionMessage(exception)));

        return new ProducerRecord<>(
                properties.getDltTopic(),
                record.partition(),
                record.timestamp(),
                record.key() == null ? null : record.key().toString(),
                record.value() == null ? null : record.value().toString(),
                headers);
    }

    private String resolveExceptionMessage(Exception exception) {
        Throwable mostSpecificCause = NestedExceptionUtils.getMostSpecificCause(exception);
        if (mostSpecificCause == null || mostSpecificCause.getMessage() == null) {
            return exception.getMessage();
        }
        return mostSpecificCause.getMessage();
    }

    private byte[] toBytes(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }
}
