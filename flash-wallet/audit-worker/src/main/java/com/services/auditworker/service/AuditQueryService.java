package com.services.auditworker.service;

import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import com.services.auditworker.dto.AuditLogResponse;
import com.services.auditworker.dto.AuditProcessingFailureResponse;
import com.services.auditworker.entity.AuditLog;
import com.services.auditworker.entity.AuditProcessingFailure;
import com.services.auditworker.repository.AuditLogRepository;
import com.services.auditworker.repository.AuditProcessingFailureRepository;

import lombok.RequiredArgsConstructor;

@Service
@EnableCaching
@RequiredArgsConstructor
public class AuditQueryService {
    
    private final AuditLogRepository auditLogRepository;
    private final AuditProcessingFailureRepository auditProcessingFailureRepository;

    @Cacheable(value = "audit-by-txn", key = "#transactionId + '-' + #pageable.pageNumber",
     condition = "#transactionId != null")
    public Page<AuditLogResponse> query(UUID transactionId, String eventType, Pageable pageable) {
        if(transactionId != null) {
            return auditLogRepository.findByTransactionId(transactionId, pageable).map(this::toResponse);
        } else if(eventType != null) {
            return auditLogRepository.findByEventType(eventType, pageable).map(this::toResponse);
        }
        return auditLogRepository.findAll(pageable).map(this::toResponse);
    }

    @Cacheable(value = "audit-by-id", key = "#id")
    public AuditLogResponse findById(UUID id) {
        Optional<AuditLog> auditLog = auditLogRepository.findById(id);
        return auditLog.map(this::toResponse).orElseThrow(() -> new TransactionalIdNotFoundException("Audit not found for id: " + id));
    }
    
    public Page<AuditProcessingFailureResponse> getFailure(Pageable pageable) {
        return auditProcessingFailureRepository.findAll(pageable).map(this::toFailureResponse);
    }

    public AuditLogResponse toResponse(AuditLog auditLog) {
        return AuditLogResponse.builder()
                .transactionId(auditLog.getTransactionId())
                .eventType(auditLog.getEventType())
                .payload(auditLog.getPayload())
                .createdAt(auditLog.getCreatedAt())
                .build();
    }

    private AuditProcessingFailureResponse toFailureResponse(AuditProcessingFailure failure) {
        return AuditProcessingFailureResponse.builder()
                .id(failure.getId())
                .topic(failure.getTopic())
                .messageKey(failure.getMessageKey())
                .kafkaPartition(failure.getKafkaPartition())
                .kafkaOffset(failure.getKafkaOffset())
                .rawPayload(failure.getRawPayload())
                .exceptionType(failure.getExceptionType())
                .exceptionMessage(failure.getExceptionMessage())
                .dltTopic(failure.getDltTopic())
                .createdAt(failure.getCreatedAt())
                .build();
    }
}
