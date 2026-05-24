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
        if (event.getTransactionId() == null) {
            throw new AuditEventValidationException("Missing required field: transactionId");
        }
        if (event.getAmount() == null) {
            throw new AuditEventValidationException("Missing required field: amount");
        }
        if (event.getAmount() <= 0L) {
            throw new AuditEventValidationException("Amount must be greater than zero");
        }
        if (!StringUtils.hasText(event.getStatus())) {
            throw new AuditEventValidationException("Missing required field: status");
        }
        if (!StringUtils.hasText(event.getEventType())) {
            throw new AuditEventValidationException("Missing required field: eventType");
        }
        if (event.getTimestamp() == null) {
            throw new AuditEventValidationException("Missing required field: timestamp");
        }

        String normalizedEventType = event.getEventType().trim();
        switch (normalizedEventType) {
            case TRANSFER_COMPLETED -> validateTransferEvent(event);
            case DEPOSIT_COMPLETED -> validateDepositEvent(event);
            default -> throw new AuditEventValidationException("Unsupported eventType: " + event.getEventType());
        }
    }

    private void validateTransferEvent(TransactionEventMessage event) {
        if (event.getSenderWalletId() == null) {
            throw new AuditEventValidationException(
                    "senderWalletId is required for eventType " + TRANSFER_COMPLETED);
        }
        if (event.getReceiverWalletId() == null) {
            throw new AuditEventValidationException(
                    "receiverWalletId is required for eventType " + TRANSFER_COMPLETED);
        }
    }

    private void validateDepositEvent(TransactionEventMessage event) {
        if (event.getReceiverWalletId() == null) {
            throw new AuditEventValidationException(
                    "receiverWalletId is required for eventType " + DEPOSIT_COMPLETED);
        }
    }
}
