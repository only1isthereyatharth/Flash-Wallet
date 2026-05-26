package com.services.wallet.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
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
    Long amount,

    @NotNull(message = "Currency is required")
    @Size(min = 3, max = 3, message = "Currency code must be exactly 3 characters")
    String currency
) {}
