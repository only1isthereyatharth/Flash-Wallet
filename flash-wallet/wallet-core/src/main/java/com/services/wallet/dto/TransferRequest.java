package com.services.wallet.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.util.UUID;

@Builder
public record TransferRequest(
    @NotNull(message = "Sender wallet ID is required")
    UUID senderWalletId,

    @NotNull(message = "Receiver wallet ID is required")
    UUID receiverWalletId,

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be strictly positive")
    @Max(value = 1_000_000_000_000L, message = "Amount exceeds maximum allowed limit")
    Long amount,

    @NotNull(message = "Currency is required")
    @CurrencyCode
    String currency
) {}
