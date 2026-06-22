package com.services.auditworker.controller;

import java.util.UUID;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.services.auditworker.dto.AuditLogResponse;
import com.services.auditworker.dto.AuditProcessingFailureResponse;
import com.services.auditworker.service.AuditQueryService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/audit")
@RequiredArgsConstructor
public class AuditQueryController {

    private final AuditQueryService auditQueryService;

    @GetMapping
    @PreAuthorize("hasAnyAuthority('ROLE_ADMIN', 'ROLE_AUDITOR')")
    public ResponseEntity<Page<AuditLogResponse>> queryAuditLogs(
            @RequestParam(required = false) UUID transactionId,
            @RequestParam(required = false) String eventType,
            Pageable pageable) {
        return ResponseEntity.ok(auditQueryService.query(transactionId, eventType, pageable));
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAnyAuthority('ROLE_ADMIN', 'ROLE_AUDITOR')")
    public ResponseEntity<AuditLogResponse> getAuditLogById(@PathVariable UUID id) {
        return ResponseEntity.ok(auditQueryService.findById(id));
    }

    @GetMapping("/failures")
    @PreAuthorize("hasAnyAuthority('ROLE_ADMIN')")
    public ResponseEntity<Page<AuditProcessingFailureResponse>> getProcessingFailures(Pageable pageable) {
        return ResponseEntity.ok(auditQueryService.getFailure(pageable));
    }
}
