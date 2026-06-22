package com.services.auditworker.dto;

import java.time.Instant;
import java.util.UUID;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record AuditLogResponse(
    @NotNull(message = "Transaction ID is required")
    UUID transactionId,

    @NotNull(message = "Event type is required")
    String eventType,

    @NotNull(message = "Payload is required")
    String payload,

    @NotNull(message = "Created at timestamp is required")
    Instant createdAt
) {}
