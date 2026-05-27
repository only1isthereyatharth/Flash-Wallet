package com.services.wallet.saga;

/**
 * Thrown when the Saga compensating transaction itself fails.
 *
 * <p>This is a critical situation: the sender was debited (Step 1), the credit
 * to the receiver failed (Step 2), and the re-credit back to the sender also
 * failed (compensation). The system is now in an inconsistent state.
 *
 * <p>This exception propagates back to the Kafka listener, triggering further
 * retries. If all retries are exhausted, the event is routed to the Saga DLT
 * ({@code wallet.saga.events.DLT}) for manual intervention by the operations team.
 */
public class SagaCompensationException extends RuntimeException {

    public SagaCompensationException(String message, Throwable cause) {
        super(message, cause);
    }
}
