package com.services.wallet.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
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
    @Size(min = 3, max = 3, message = "Currency code must be exactly 3 characters")
    String currency,

    @NotNull(message = "Version is required")
    Integer version,

    @NotNull(message = "Updated at timestamp is required")
    Instant updatedAt
) {}
