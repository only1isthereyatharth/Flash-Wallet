package com.services.auditworker.dto;

import java.time.Instant;
import java.util.UUID;
import lombok.Builder;

@Builder
public record AuditProcessingFailureResponse(
    UUID id,
    String topic,
    String messageKey,
    int kafkaPartition,
    long kafkaOffset,
    String rawPayload,
    String exceptionType,
    String exceptionMessage,
    String dltTopic,
    Instant createdAt
) {}
