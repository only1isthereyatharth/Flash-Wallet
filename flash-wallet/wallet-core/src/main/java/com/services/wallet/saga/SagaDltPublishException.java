package com.services.wallet.saga;

/**
 * Thrown when publishing a failed saga event to the Dead Letter Topic itself fails.
 *
 * <p>This is a critical error — it means the saga event could not be saved to the DLT
 * and may be lost. Operational alerting on this exception is strongly recommended.
 */
public class SagaDltPublishException extends RuntimeException {

    public SagaDltPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
