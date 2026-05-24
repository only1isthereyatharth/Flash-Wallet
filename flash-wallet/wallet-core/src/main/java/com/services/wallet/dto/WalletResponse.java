package com.services.wallet.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WalletResponse {
    private UUID id;
    private UUID userId;
    private Long balance; // lowest unit (Paisa)
    private String currency;
    private Integer version;
    private Instant updatedAt;
}
