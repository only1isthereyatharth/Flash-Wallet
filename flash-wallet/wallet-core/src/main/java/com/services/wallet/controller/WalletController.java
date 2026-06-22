package com.services.wallet.controller;

import com.services.wallet.dto.*;
import com.services.wallet.idempotency.Idempotent;
import com.services.wallet.repository.TransactionRepository;
import com.services.wallet.service.WalletService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/wallets")
@Validated
@RequiredArgsConstructor
@Slf4j
public class WalletController {

    private final WalletService walletService;
    private final TransactionRepository transactionRepository;
    /**
     * No need for manual wallet creation since wallet creation is triggered automatically when user registers
     */

    /**
     * Initiates a P2P transfer via the Saga pattern.
     *
     * <p>Returns {@code 202 Accepted} immediately after the sender is debited
     * and the saga event is published. The receiver credit happens asynchronously.
     * Poll {@code GET /api/v1/wallets/transactions/{transactionId}} with the returned
     * {@code transactionId} to determine the final outcome ({@code PENDING → SUCCESS | FAILED}).
     */
    @PostMapping("/transfer")
    @Idempotent
    @PreAuthorize(
        "hasAuthority('SCOPE_internal:debit') or " +
        "(hasAuthority('SCOPE_wallet:write') and @walletOwnershipGuard.assertWalletOwnership(#request.senderWalletId(), #auth))"
    )
    public ResponseEntity<TransferResponse> transfer(
            @RequestBody @Valid TransferRequest request,
            HttpServletRequest httpServletRequest, Authentication auth) {
        
        String idempotencyKey = httpServletRequest.getHeader("Idempotency-Key");
        log.info("REST: Idempotent transfer request with key: {}", idempotencyKey);
        TransferResponse response = walletService.transfer(request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @PostMapping("/deposit")
    @Idempotent
    @PreAuthorize(
        "hasAuthority('SCOPE_internal:credit') or " +
        "(hasAuthority('SCOPE_wallet:write') and @walletOwnershipGuard.assertWalletOwnership(#request.walletId(), #auth))"
    )
    public ResponseEntity<WalletResponse> deposit(
        @RequestBody @Valid DepositRequest request,
        HttpServletRequest httpServletRequest, Authentication auth) {
            
        String idempotencyKey = httpServletRequest.getHeader("Idempotency-Key");
        log.info("REST: Idempotent deposit request with key: {}", idempotencyKey);
        
        WalletResponse response = walletService.deposit(request, idempotencyKey);
        return ResponseEntity.ok(response);
    }
        
    @GetMapping("/{walletId}")
    @PreAuthorize(
        "hasAuthority('SCOPE_wallet:read_all') or " +
        "(hasAuthority('SCOPE_wallet:read') and @walletOwnershipGuard.assertWalletOwnership(#walletId, #auth))"
    )
    public ResponseEntity<WalletResponse> getWallet(@PathVariable UUID walletId, Authentication auth) {

        log.info("REST: Fetching wallet details for wallet ID: {}", walletId);
        WalletResponse response = walletService.getWallet(walletId);
        return ResponseEntity.ok(response);
    }


    @GetMapping("/user/{userId}")
    @PreAuthorize(
        "hasAuthority('SCOPE_wallet:read_all') or " +
        "(hasAuthority('SCOPE_wallet:read') and @walletOwnershipGuard.assertUserIdMatch(#userId, #auth))"
    )
    public ResponseEntity<WalletResponse> getWalletByUserId(@PathVariable UUID userId, Authentication auth) {

        log.info("REST: Fetching wallet details for user ID: {}", userId);
        WalletResponse response = walletService.getWalletByUserId(userId);
        return ResponseEntity.ok(response);
    }

    /**
     * Polls the current status of a transfer transaction.
     *
     * <p>Use the {@code transactionId} returned by {@code POST /transfer} to check
     * whether the Saga completed ({@code SUCCESS}), failed ({@code FAILED}),
     * or is still in-flight ({@code PENDING}).
     *
     * @param transactionId the UUID returned by the transfer endpoint
     * @return 200 with status payload, or 404 if the transaction does not exist
     */
    @GetMapping("/transactions/{transactionId}")
    @PreAuthorize(
        "hasAuthority('SCOPE_transaction:read_all') or " +
        "(hasAuthority('SCOPE_transaction:read') and @walletOwnershipGuard.assertTransactionIdMatch(#transactionId, #auth))"
    )
    public ResponseEntity<TransactionStatusResponse> getTransactionStatus(
            @PathVariable UUID transactionId, Authentication auth) {
        log.info("REST: Polling status for transactionId: {}", transactionId);
        return transactionRepository.findById(transactionId)
                .map(tx -> ResponseEntity.ok(TransactionStatusResponse.builder()
                        .transactionId(tx.getId())
                        .status(tx.getStatus().name())
                        .build()))
                .orElse(ResponseEntity.notFound().build());
    }
}
