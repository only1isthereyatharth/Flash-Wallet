package com.services.wallet.idempotency;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IdempotencyState implements Serializable {
    private static final long serialVersionUID = 2L;

    private String status; // "PROCESSING" or "COMPLETED"
    private String responseBody; // JSON serialized response
    private int statusCode; // Cached HTTP status code (e.g. 200)
    private String payloadHash; // SHA-256 of the original request body
}
