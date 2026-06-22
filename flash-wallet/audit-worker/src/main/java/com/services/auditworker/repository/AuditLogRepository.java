package com.services.auditworker.repository;

import com.services.auditworker.entity.AuditLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AuditLogRepository extends JpaRepository<AuditLog, UUID> {

    Page<AuditLog> findByTransactionId(UUID transactionId, Pageable pageable);

    Page<AuditLog> findByEventType(String eventType, Pageable pageable);
}
