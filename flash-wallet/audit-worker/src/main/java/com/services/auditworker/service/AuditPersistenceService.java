package com.services.auditworker.service;

import com.services.auditworker.dto.TransactionEventMessage;
import com.services.auditworker.entity.AuditLog;
import com.services.auditworker.exception.AuditPersistenceException;
import com.services.auditworker.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditPersistenceService {

    private static final String DUPLICATE_AUDIT_LOG_CONSTRAINT = "uk_audit_logs_partition_offset";

    private final AuditLogRepository auditLogRepository;

    @Transactional
    public void persist(ConsumerRecord<String, String> record, TransactionEventMessage event, String rawPayload) {
        AuditLog auditLog = AuditLog.builder()
                .transactionId(event.transactionId())
                .eventType(event.eventType())
                .payload(rawPayload)
                .kafkaPartition(record.partition())
                .kafkaOffset(record.offset())
                .build();

        try {
            AuditLog savedAuditLog = auditLogRepository.saveAndFlush(auditLog);
            log.info(
                    "Audit log persisted successfully: auditLogId={}, transactionId={}, topic={}, partition={}, offset={}, eventType={}",
                    savedAuditLog.getId(),
                    event.transactionId(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    event.eventType());
        } catch (RuntimeException ex) {
            if (PersistenceExceptionClassifier.isDuplicateConstraint(ex, DUPLICATE_AUDIT_LOG_CONSTRAINT)) {
                log.warn(
                        "Duplicate audit redelivery ignored: transactionId={}, topic={}, partition={}, offset={}, key={}",
                        event.transactionId(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key());
                return;
            }

            log.error(
                    "Failed to persist audit log: transactionId={}, topic={}, partition={}, offset={}, key={}",
                    event.transactionId(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    ex);
            throw new AuditPersistenceException(
                    "Failed to persist audit log for topic "
                            + record.topic()
                            + ", partition "
                            + record.partition()
                            + ", offset "
                            + record.offset(),
                    ex);
        }
    }
}
