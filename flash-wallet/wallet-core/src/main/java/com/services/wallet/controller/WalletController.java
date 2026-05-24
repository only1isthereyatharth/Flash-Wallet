package com.services.wallet.controller;

import com.services.wallet.dto.*;
import com.services.wallet.idempotency.Idempotent;
import com.services.wallet.service.WalletService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/wallets")
@RequiredArgsConstructor
@Slf4j
public class WalletController {

    private final WalletService walletService;

    @PostMapping
    public ResponseEntity<WalletResponse> createWallet(@RequestBody @Valid CreateWalletRequest request) {
        log.info("REST: Request to create wallet for user: {}", request.getUserId());
        WalletResponse response = walletService.createWallet(request.getUserId(), request.getCurrency());
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/transfer")
    @Idempotent
    public ResponseEntity<TransferResponse> transfer(
            @RequestBody @Valid TransferRequest request,
            HttpServletRequest httpServletRequest) {
        String idempotencyKey = httpServletRequest.getHeader("Idempotency-Key");
        log.info("REST: Idempotent transfer request with key: {}", idempotencyKey);
        TransferResponse response = walletService.transfer(request, idempotencyKey);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/deposit")
    @Idempotent
    public ResponseEntity<WalletResponse> deposit(
            @RequestBody @Valid DepositRequest request,
            HttpServletRequest httpServletRequest) {
        String idempotencyKey = httpServletRequest.getHeader("Idempotency-Key");
        log.info("REST: Idempotent deposit request with key: {}", idempotencyKey);
        WalletResponse response = walletService.deposit(request, idempotencyKey);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{walletId}")
    public ResponseEntity<WalletResponse> getWallet(@PathVariable UUID walletId) {
        log.info("REST: Fetching wallet details for wallet ID: {}", walletId);
        WalletResponse response = walletService.getWallet(walletId);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/user/{userId}")
    public ResponseEntity<WalletResponse> getWalletByUserId(@PathVariable UUID userId) {
        log.info("REST: Fetching wallet details for user ID: {}", userId);
        WalletResponse response = walletService.getWalletByUserId(userId);
        return ResponseEntity.ok(response);
    }
}
