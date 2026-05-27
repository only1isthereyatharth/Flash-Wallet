package com.services.wallet.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;

import java.util.UUID;

@Builder
public record CreateWalletRequest(
    @NotNull(message = "User ID is required")
    UUID userId,

    @NotNull(message = "Currency is required")
    @Size(min = 3, max = 3, message = "Currency code must be exactly 3 characters")
    String currency
) {}
