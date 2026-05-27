package com.services.auditworker.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.auditworker.exception.AuditEventDeserializationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.DeserializationFeature;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AuditEventProcessingServiceTest {

    @Test
    void shouldThrowDeserializationExceptionForMalformedJson() {
        AuditEventValidationService validationService = new AuditEventValidationService();
        AuditPersistenceService persistenceService = new AuditPersistenceService(null) {
            @Override
            public void persist(ConsumerRecord<String, String> record,
                                com.services.auditworker.dto.TransactionEventMessage event,
                                String rawPayload) {
                throw new AssertionError("Persistence should not be reached for malformed JSON");
            }
        };
        AuditEventProcessingService processingService = new AuditEventProcessingService(
                new ObjectMapper().findAndRegisterModules()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true),
                validationService,
                persistenceService);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "wallet.transaction.events",
                0,
                12L,
                "transaction-key",
                "{\"transactionId\":");

        assertThrows(AuditEventDeserializationException.class, () -> processingService.process(record));
    }
}
