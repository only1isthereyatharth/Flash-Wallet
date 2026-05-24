package com.services.auditworker.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEventMessage {

    private UUID transactionId;
    private String idempotencyKey;
    private UUID senderWalletId;
    private UUID receiverWalletId;
    private Long amount;
    private String status;
    private String eventType;
    private Instant timestamp;
}
