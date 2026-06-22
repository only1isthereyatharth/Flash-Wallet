package com.services.wallet.service;

import org.springframework.security.access.AccessDeniedException;
import java.util.UUID;

import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import com.services.wallet.exception.WalletNotFoundException;
import com.services.wallet.model.Wallet;
import com.services.wallet.repository.TransactionRepository;
import com.services.wallet.repository.WalletRepository;

import lombok.RequiredArgsConstructor;

@Component("walletOwnershipGuard")
@RequiredArgsConstructor
public class WalletOwnershipGuard {

    private final WalletRepository walletRepository;
    private final TransactionRepository transactionRepository;

    /** 
     * Called by controller when the authenticated user is CUSTOMER.
     * Fetches wallet by walletId, checks if wallet.userId == auth principal (X-User-Id).
     */
    public boolean assertWalletOwnership(UUID walletId, Authentication auth) {
        Wallet wallet = walletRepository.findById(walletId)
            .orElseThrow(() -> new WalletNotFoundException("Wallet not found: " + walletId));
        if (!wallet.getUserId().toString().equals(auth.getName())) {
            throw new AccessDeniedException("Access denied: wallet does not belong to current user");
        }
        return true;
    }

    /**
     * For endpoints that take userId directly (e.g. createWallet, getWalletByUserId).
     */
    public boolean assertUserIdMatch(UUID requestedUserId, Authentication auth) {
        if (!requestedUserId.toString().equals(auth.getName())) {
            throw new AccessDeniedException("Access denied: userId does not match authenticated user");
        }
        return true;
    }

    public boolean assertTransactionIdMatch(UUID transactionId, Authentication auth) {
        // checks transaction from auth userId, if transaction is by this user or not
        UUID userId = UUID.fromString(auth.getName());
        boolean exists = transactionRepository.existsByTransactionIdAndUserId(transactionId, userId);
        if (!exists) {
            throw new AccessDeniedException("Access denied: transaction does not belong to current user");
        }
        return true;
    }
}
