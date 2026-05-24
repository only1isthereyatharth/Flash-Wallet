package com.services.auditworker.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.auditworker.dto.TransactionEventMessage;
import com.services.auditworker.exception.AuditEventDeserializationException;
import com.services.auditworker.exception.AuditEventValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditEventProcessingService {

    private final ObjectMapper objectMapper;
    private final AuditEventValidationService auditEventValidationService;
    private final AuditPersistenceService auditPersistenceService;

    public void process(ConsumerRecord<String, String> record) {
        TransactionEventMessage event = deserialize(record);

        log.info(
                "Audit event parsed successfully: transactionId={}, eventType={}, topic={}, partition={}, offset={}",
                event.getTransactionId(),
                event.getEventType(),
                record.topic(),
                record.partition(),
                record.offset());

        try {
            auditEventValidationService.validate(event);
        } catch (AuditEventValidationException ex) {
            log.warn(
                    "Audit event validation failed: transactionId={}, topic={}, partition={}, offset={}, key={}, reason={}",
                    event.getTransactionId(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    ex.getMessage());
            throw ex;
        }

        log.info(
                "Audit event validated successfully: transactionId={}, eventType={}, topic={}, partition={}, offset={}",
                event.getTransactionId(),
                event.getEventType(),
                record.topic(),
                record.partition(),
                record.offset());

        auditPersistenceService.persist(record, event, record.value());
    }

    private TransactionEventMessage deserialize(ConsumerRecord<String, String> record) {
        try {
            return objectMapper.readValue(record.value(), TransactionEventMessage.class);
        } catch (JsonProcessingException ex) {
            log.warn(
                    "Audit event deserialization failed: topic={}, partition={}, offset={}, key={}, message={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    ex.getOriginalMessage());
            throw new AuditEventDeserializationException(
                    "Failed to deserialize audit event from topic "
                            + record.topic()
                            + ", partition "
                            + record.partition()
                            + ", offset "
                            + record.offset(),
                    ex);
        }
    }
}
