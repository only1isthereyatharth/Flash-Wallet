package com.services.wallet.dto;

import lombok.Builder;

import java.util.UUID;

@Builder
public record TransactionStatusResponse(
    UUID transactionId,
    String status
) {}
