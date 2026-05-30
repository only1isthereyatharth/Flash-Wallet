package com.services.wallet.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record WalletResponse(
    @NotNull(message = "Wallet ID is required")
    UUID id,

    @NotNull(message = "User ID is required")
    UUID userId,

    @NotNull(message = "Balance is required")
    @PositiveOrZero(message = "Balance must be positive or zero")
    Long balance,

    @NotBlank(message = "Currency is required")
    @CurrencyCode
    String currency,

    @NotNull(message = "Updated at timestamp is required")
    Instant updatedAt
) {}
