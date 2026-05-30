package com.services.auditworker.service;

import com.services.auditworker.dto.TransactionEventMessage;
import com.services.auditworker.exception.AuditEventValidationException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AuditEventValidationServiceTest {

    private final AuditEventValidationService validationService = new AuditEventValidationService();

    @Test
    void shouldAcceptValidTransferEvent() {
        TransactionEventMessage event = TransactionEventMessage.builder()
                .transactionId(UUID.randomUUID())
                .senderWalletId(UUID.randomUUID())
                .receiverWalletId(UUID.randomUUID())
                .amount(5000L)
                .currency("INR")
                .status("SUCCESS")
                .eventType("WALLET_TRANSFER_COMPLETED")
                .timestamp(Instant.now())
                .build();

        assertDoesNotThrow(() -> validationService.validate(event));
    }

    @Test
    void shouldAcceptValidDepositEventWithNullSender() {
        TransactionEventMessage event = TransactionEventMessage.builder()
                .transactionId(UUID.randomUUID())
                .receiverWalletId(UUID.randomUUID())
                .amount(2500L)
                .currency("INR")
                .status("SUCCESS")
                .eventType("WALLET_DEPOSIT_COMPLETED")
                .timestamp(Instant.now())
                .build();

        assertDoesNotThrow(() -> validationService.validate(event));
    }

    @Test
    void shouldRejectUnknownEventType() {
        TransactionEventMessage event = TransactionEventMessage.builder()
                .transactionId(UUID.randomUUID())
                .receiverWalletId(UUID.randomUUID())
                .amount(2500L)
                .currency("INR")
                .status("SUCCESS")
                .eventType("UNKNOWN_EVENT")
                .timestamp(Instant.now())
                .build();

        assertThrows(AuditEventValidationException.class, () -> validationService.validate(event));
    }

    @Test
    void shouldRejectTransferWithoutReceiverWallet() {
        TransactionEventMessage event = TransactionEventMessage.builder()
                .transactionId(UUID.randomUUID())
                .senderWalletId(UUID.randomUUID())
                .amount(2500L)
                .currency("INR")
                .status("SUCCESS")
                .eventType("WALLET_TRANSFER_COMPLETED")
                .timestamp(Instant.now())
                .build();

        assertThrows(AuditEventValidationException.class, () -> validationService.validate(event));
    }

    @Test
    void shouldAcceptValidSagaFailedEvent() {
        TransactionEventMessage event = TransactionEventMessage.builder()
                .transactionId(UUID.randomUUID())
                .senderWalletId(UUID.randomUUID())
                .receiverWalletId(UUID.randomUUID())
                .amount(7500L)
                .currency("INR")
                .status("COMPENSATED")
                .eventType("WALLET_TRANSFER_SAGA_FAILED")
                .timestamp(Instant.now())
                .build();

        assertDoesNotThrow(() -> validationService.validate(event));
    }

    @Test
    void shouldRejectSagaFailedWithoutSenderWallet() {
        TransactionEventMessage event = TransactionEventMessage.builder()
                .transactionId(UUID.randomUUID())
                .receiverWalletId(UUID.randomUUID())
                .amount(7500L)
                .currency("INR")
                .status("COMPENSATED")
                .eventType("WALLET_TRANSFER_SAGA_FAILED")
                .timestamp(Instant.now())
                .build();

        assertThrows(AuditEventValidationException.class, () -> validationService.validate(event));
    }
}
