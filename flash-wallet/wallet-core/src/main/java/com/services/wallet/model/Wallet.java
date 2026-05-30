package com.services.wallet.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "wallets")
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Wallet {

    @Id
    private UUID id;

    @Column(name = "user_id", unique = true, nullable = false)
    private UUID userId;

    @Column(name = "balance", nullable = false)
    @PositiveOrZero(message = "Balance should be greater than or equal to 0")
    private Long balance; // Stored in lowest unit (Paisa/Cents)

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Version
    @Column(name = "version", nullable = false)
    private Integer version;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;
}
