package com.services.wallet.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;

import java.util.UUID;

@Builder
public record CreateWalletRequest(
    @NotNull(message = "User ID is required")
    UUID userId,

    @NotNull(message = "Currency is required")
    @CurrencyCode
    String currency
) {}
