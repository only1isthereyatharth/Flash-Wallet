package com.services.wallet.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.util.UUID;

@Builder
public record TransferResponse(
    @NotNull(message = "Transaction ID is required")
    UUID transactionId,

    @NotNull(message = "Sender wallet ID is required")
    UUID senderWalletId,

    @NotNull(message = "Receiver wallet ID is required")
    UUID receiverWalletId,

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be strictly positive")
    Long amount,

    @NotBlank(message = "Status is required")
    String status,

    String message
) {}
