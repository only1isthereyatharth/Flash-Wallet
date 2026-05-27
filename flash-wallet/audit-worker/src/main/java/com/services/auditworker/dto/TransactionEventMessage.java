package com.services.auditworker.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record TransactionEventMessage(
    @NotNull(message = "Transaction ID is required")
    UUID transactionId,

    @NotBlank(message = "Idempotency key is required")
    String idempotencyKey,

    UUID senderWalletId, // can be null for external deposit

    @NotNull(message = "Receiver wallet ID is required")
    UUID receiverWalletId,

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be strictly positive")
    Long amount,

    @NotBlank(message = "Currency is required")
    String currency,

    @NotBlank(message = "Status is required")
    String status,

    @NotBlank(message = "Event type is required")
    String eventType,

    @NotNull(message = "Timestamp is required")
    Instant timestamp
) {}
