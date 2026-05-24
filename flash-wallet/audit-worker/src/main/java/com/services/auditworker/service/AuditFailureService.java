package com.services.auditworker.service;

import com.services.auditworker.entity.AuditProcessingFailure;
import com.services.auditworker.repository.AuditProcessingFailureRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditFailureService {

    private static final String DUPLICATE_FAILURE_CONSTRAINT =
            "uk_audit_processing_failures_topic_partition_offset";

    private final AuditProcessingFailureRepository auditProcessingFailureRepository;

    @Transactional
    public void recordRecoveredFailure(ConsumerRecord<?, ?> record, Exception exception, String dltTopic) {
        AuditProcessingFailure failure = AuditProcessingFailure.builder()
                .topic(record.topic())
                .messageKey(record.key() == null ? null : record.key().toString())
                .kafkaPartition(record.partition())
                .kafkaOffset(record.offset())
                .rawPayload(record.value() == null ? null : record.value().toString())
                .exceptionType(exception.getClass().getName())
                .exceptionMessage(resolveExceptionMessage(exception))
                .dltTopic(dltTopic)
                .build();

        try {
            auditProcessingFailureRepository.saveAndFlush(failure);
            log.info(
                    "Audit failure record persisted successfully: topic={}, partition={}, offset={}, key={}, dltTopic={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    dltTopic);
        } catch (RuntimeException ex) {
            if (PersistenceExceptionClassifier.isDuplicateConstraint(ex, DUPLICATE_FAILURE_CONSTRAINT)) {
                log.warn(
                        "Duplicate audit failure record ignored: topic={}, partition={}, offset={}, key={}, dltTopic={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        dltTopic);
                return;
            }

            log.error(
                    "Failed to persist audit failure record after DLT recovery: topic={}, partition={}, offset={}, key={}, dltTopic={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    dltTopic,
                    ex);
        }
    }

    private String resolveExceptionMessage(Exception exception) {
        Throwable mostSpecificCause = NestedExceptionUtils.getMostSpecificCause(exception);
        if (mostSpecificCause == null) {
            return exception.getMessage();
        }
        return mostSpecificCause.getMessage();
    }
}
