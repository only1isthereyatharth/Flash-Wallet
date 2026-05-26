package com.services.wallet.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransactionEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private UUID transactionId;
    private String idempotencyKey;
    private UUID senderWalletId;
    private UUID receiverWalletId;
    private Long amount; // stored in Paisa/Cents
    private String currency;
    private String status;
    private String eventType;
    private Instant timestamp;
}
