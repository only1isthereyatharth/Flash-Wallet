package com.services.wallet.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransferRequest {

    @NotNull(message = "Sender wallet ID is required")
    private UUID senderWalletId;

    @NotNull(message = "Receiver wallet ID is required")
    private UUID receiverWalletId;

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be strictly positive")
    private Long amount; // in lowest currency unit (Paisa)

    @NotNull(message = "Currency is required")
    @Size(min = 3, max = 3, message = "Currency code must be exactly 3 characters")
    private String currency;
}
