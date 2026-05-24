package com.services.wallet.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransferResponse {
    private UUID transactionId;
    private UUID senderWalletId;
    private UUID receiverWalletId;
    private Long amount;
    private String status;
    private String message;
}
