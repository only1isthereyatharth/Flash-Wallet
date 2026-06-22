package com.services.wallet.repository;

import com.services.wallet.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<Transaction, UUID> {
    Optional<Transaction> findByIdempotencyKey(String idempotencyKey);

    @Query("""
        select COUNT(*) > 0 from transactions t
        JOIN wallets w ON (w.id = t.sender_wallet_id OR w.id = t.receiver_wallet_id)
        where t.id = :transactionId and w.user_id = :userId
    """)
    boolean existsByTransactionIdAndUserId(@Param("transactionId") UUID transactionId, @Param("userId") UUID userId);
}
