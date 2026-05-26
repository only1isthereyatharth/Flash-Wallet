package com.services.auditworker.service;

import com.services.auditworker.dto.TransactionEventMessage;
import com.services.auditworker.exception.AuditEventValidationException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class AuditEventValidationService {

    private static final String TRANSFER_COMPLETED = "WALLET_TRANSFER_COMPLETED";
    private static final String DEPOSIT_COMPLETED = "WALLET_DEPOSIT_COMPLETED";

    public void validate(TransactionEventMessage event) {
        if (event.transactionId() == null) {
            throw new AuditEventValidationException("Missing required field: transactionId");
        }
        if (event.amount() == null) {
            throw new AuditEventValidationException("Missing required field: amount");
        }
        if (event.amount() <= 0L) {
            throw new AuditEventValidationException("Amount must be greater than zero");
        }
        if (!StringUtils.hasText(event.status())) {
            throw new AuditEventValidationException("Missing required field: status");
        }
        if (!StringUtils.hasText(event.eventType())) {
            throw new AuditEventValidationException("Missing required field: eventType");
        }
        if (event.timestamp() == null) {
            throw new AuditEventValidationException("Missing required field: timestamp");
        }

        String normalizedEventType = event.eventType().trim();
        switch (normalizedEventType) {
            case TRANSFER_COMPLETED -> validateTransferEvent(event);
            case DEPOSIT_COMPLETED -> validateDepositEvent(event);
            default -> throw new AuditEventValidationException("Unsupported eventType: " + event.eventType());
        }
    }

    private void validateTransferEvent(TransactionEventMessage event) {
        if (event.senderWalletId() == null) {
            throw new AuditEventValidationException(
                    "senderWalletId is required for eventType " + TRANSFER_COMPLETED);
        }
        if (event.receiverWalletId() == null) {
            throw new AuditEventValidationException(
                    "receiverWalletId is required for eventType " + TRANSFER_COMPLETED);
        }
    }

    private void validateDepositEvent(TransactionEventMessage event) {
        if (event.receiverWalletId() == null) {
            throw new AuditEventValidationException(
                    "receiverWalletId is required for eventType " + DEPOSIT_COMPLETED);
        }
    }
}
