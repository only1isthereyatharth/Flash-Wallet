package com.services.auditworker.repository;

import com.services.auditworker.entity.AuditProcessingFailure;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AuditProcessingFailureRepository extends JpaRepository<AuditProcessingFailure, UUID> {
}
