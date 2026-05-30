package com.services.wallet.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transactions")
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Transaction {

    @Id
    private UUID id;

    @Column(name = "idempotency_key", unique = true, nullable = false, length = 100)
    private String idempotencyKey;

    @Column(name = "sender_wallet_id")
    private UUID senderWalletId;

    @Column(name = "receiver_wallet_id")
    private UUID receiverWalletId;

    @Column(name = "amount", nullable = false)
    private Long amount; // Stored in lowest unit (Paisa/Cents)

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private TransactionStatus status;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private Instant createdAt;
}
