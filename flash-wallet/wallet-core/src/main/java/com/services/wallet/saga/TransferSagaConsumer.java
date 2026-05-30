package com.services.wallet.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.wallet.event.TransactionEvent;
import com.services.wallet.exception.WalletNotFoundException;
import com.services.wallet.lock.LockManager;
import com.services.wallet.model.Transaction;
import com.services.wallet.model.TransactionStatus;
import com.services.wallet.model.Wallet;
import com.services.wallet.producer.WalletEventProducer;
import com.services.wallet.repository.TransactionRepository;
import com.services.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hibernate.validator.internal.util.stereotypes.Lazy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Choreography-based Saga consumer for wallet P2P transfers.
 *
 * <h2>Saga Flow</h2>
 * 
 * <pre>
 * Step 1 (WalletService):
 *   – Debit sender, save Transaction(status=DEBIT_COMPLETED)
 *   – Publish TRANSFER_DEBIT_COMPLETED → wallet.saga.events
 *
 * Step 2 (this consumer — happy path):
 *   – Receive TRANSFER_DEBIT_COMPLETED
 *   – Credit receiver, update Transaction(status=COMPLETED)
 *   – Publish TRANSFER_COMPLETED → wallet.transaction.events
 *
 * Compensation (this consumer — failure path):
 *   – Re-credit sender, update Transaction(status=COMPENSATED)
 *   – Publish TRANSFER_SAGA_FAILED → wallet.transaction.events
 *
 * DLT exhaustion (compensation retries exhausted):
 *   – Transaction marked as FAILED — manual intervention required
 * </pre>
 *
 * <h2>Idempotency</h2>
 * <p>
 * Before applying the credit or compensation, the consumer checks
 * {@code Transaction.status}. If the status is not {@code DEBIT_COMPLETED},
 * the event is a duplicate delivery and is skipped silently.
 * This makes both the credit and the compensation idempotent.
 *
 * <h2>Concurrency &amp; Locks</h2>
 * <p>
 * The existing {@link LockManager} is reused. For the credit step, only
 * the receiver wallet is locked. The sender was already unlocked after Step 1.
 * For compensation, only the sender wallet is re-locked.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransferSagaConsumer {

        private static final String EVENT_DEBIT_COMPLETED = "TRANSFER_DEBIT_COMPLETED";

        private final ObjectMapper objectMapper;
        private final WalletRepository walletRepository;
        private final TransactionRepository transactionRepository;
        private final WalletEventProducer eventProducer;
        private final LockManager lockManager;

        @Autowired
        @Lazy
        private TransferSagaConsumer self;

        /**
         * Saga Step 2: listens for debit-completed events from Step 1 and
         * orchestrates the credit or compensation transactionally.
         */
        @KafkaListener(topics = "${wallet.saga.topic}", containerFactory = "sagaKafkaListenerContainerFactory")
        public void onSagaEvent(ConsumerRecord<String, String> record) {
                log.info(
                                "Received saga event: topic={}, partition={}, offset={}, key={}",
                                record.topic(), record.partition(), record.offset(), record.key());

                TransactionEvent event = deserialize(record);

                if (!EVENT_DEBIT_COMPLETED.equals(event.eventType())) {
                        log.info(
                                        "Saga consumer ignoring unsupported eventType={} for transactionId={}",
                                        event.eventType(), event.transactionId());
                        return;
                }

                log.info(
                                "Processing TRANSFER_DEBIT_COMPLETED: transactionId={}, senderWalletId={}, receiverWalletId={}, amount={}",
                                event.transactionId(), event.senderWalletId(),
                                event.receiverWalletId(), event.amount());

                // Guard: check for duplicate delivery before acquiring any lock
                Transaction transaction = transactionRepository.findById(event.transactionId())
                                .orElseThrow(() -> {
                                        log.error("Saga Step 2 aborted: Transaction record not found for id={}",
                                                        event.transactionId());
                                        // Throwing here triggers Kafka retry → DLT after exhaustion
                                        return new IllegalStateException(
                                                        "Transaction record not found for saga event: "
                                                                        + event.transactionId());
                                });

                if (transaction.getStatus() != TransactionStatus.DEBIT_COMPLETED) {
                        log.info(
                                        "Saga event is a duplicate delivery — already processed. " +
                                                        "transactionId={}, currentStatus={}, skipping.",
                                        event.transactionId(), transaction.getStatus());
                        return;
                }

                // Execute credit under receiver wallet lock
                try {
                        lockManager.executeWithLock(
                                        event.receiverWalletId(),
                                        5000, // 5s max wait — matches Step 1 lock timeout
                                        10000, // 10s lease guard
                                        () -> {
                                                self.executeCreditTx(event, transaction);
                                                return null;
                                        });
                } catch (Exception creditException) {
                        // Credit failed — initiate compensation under sender wallet lock
                        log.error(
                                        "Saga Step 2 (credit) failed for transactionId={}. Initiating compensation.",
                                        event.transactionId(), creditException);
                        try {
                                lockManager.executeWithLock(
                                                event.senderWalletId(),
                                                5000,
                                                10000,
                                                () -> {
                                                        self.executeCompensationTx(event, transaction);
                                                        return null;
                                                });
                        } catch (Exception compensationException) {
                                log.error(
                                                "Saga compensation also failed for transactionId={}. " +
                                                                "Event will be retried by Kafka and may end in DLT. " +
                                                                "MANUAL INTERVENTION MAY BE REQUIRED.",
                                                event.transactionId(), compensationException);
                                // Re-throw so Kafka retry/DLT takes over
                                throw new SagaCompensationException(
                                                "Saga compensation failed for transactionId: "
                                                                + event.transactionId(),
                                                compensationException);
                        }
                }
        }

        /**
         * Applies the receiver credit within the caller's lock scope.
         * Must be called inside an active lock on the receiver wallet.
         */
        @Transactional
        public void executeCreditTx(TransactionEvent event, Transaction transaction) {
                Wallet receiver = walletRepository.findById(event.receiverWalletId())
                                .orElseThrow(() -> new WalletNotFoundException(
                                                "Receiver wallet not found during saga credit: "
                                                                + event.receiverWalletId()));

                // Validate currency consistency — guards against wallet being recreated with a
                // different currency
                if (!receiver.getCurrency().equalsIgnoreCase(event.currency())) {
                        throw new IllegalStateException(String.format(
                                        "Currency mismatch in saga credit step: expected=%s, receiverWalletCurrency=%s, transactionId=%s",
                                        event.currency(), receiver.getCurrency(), event.transactionId()));
                }

                // Overflow guard: ensure balance + amount doesn't exceed Long.MAX_VALUE
                if (receiver.getBalance() > Long.MAX_VALUE - event.amount()) {
                        throw new IllegalStateException("Credit would overflow receiver wallet balance for transactionId: " + event.transactionId());
                }

                receiver.setBalance(receiver.getBalance() + event.amount());
                walletRepository.save(receiver);

                transaction.setStatus(TransactionStatus.COMPLETED);
                transactionRepository.save(transaction);

                // Publish terminal success event to the shared topic (consumed by audit-worker
                // and any future services)
                TransactionEvent completedEvent = TransactionEvent.builder()
                                .transactionId(event.transactionId())
                                .idempotencyKey(event.idempotencyKey())
                                .senderWalletId(event.senderWalletId())
                                .receiverWalletId(event.receiverWalletId())
                                .amount(event.amount())
                                .currency(event.currency())
                                .status(TransactionStatus.COMPLETED.name())
                                .eventType("TRANSFER_COMPLETED")
                                .timestamp(Instant.now())
                                .build();
                eventProducer.sendTransactionEvent(completedEvent);

                log.info(
                                "Saga Step 2 (credit) committed: transactionId={}, receiverWalletId={}, amount={}",
                                event.transactionId(), event.receiverWalletId(), event.amount());
        }

        /**
         * Compensating transaction: re-credits the sender to restore the pre-saga
         * balance.
         * Must be called inside an active lock on the sender wallet.
         *
         * <p>
         * <b>Idempotency guard</b>: If {@code Transaction.status} is already
         * {@code COMPENSATED} or {@code FAILED}, compensation was already applied
         * on a previous retry — skip silently.
         */
        @Transactional
        public void executeCompensationTx(TransactionEvent event, Transaction transaction) {
                // Reload transaction inside the new tx to get the freshest status
                Transaction freshTx = transactionRepository.findById(event.transactionId())
                                .orElseThrow(() -> new IllegalStateException(
                                                "Transaction not found during saga compensation: "
                                                                + event.transactionId()));

                if (freshTx.getStatus() == TransactionStatus.COMPENSATED
                    || freshTx.getStatus() == TransactionStatus.FAILED) {
                        log.info(
                                        "Compensation already applied (idempotent skip): transactionId={}, status={}",
                                        event.transactionId(), freshTx.getStatus());
                        return;
                }

                Wallet sender = walletRepository.findById(event.senderWalletId())
                                .orElseThrow(() -> new WalletNotFoundException(
                                                "Sender wallet not found during saga compensation: "
                                                                + event.senderWalletId()));

                sender.setBalance(sender.getBalance() + event.amount());
                walletRepository.save(sender);

                freshTx.setStatus(TransactionStatus.COMPENSATED);
                transactionRepository.save(freshTx);

                // Publish terminal rollback event — audit-worker will record this, ops can alert
                // on it
                TransactionEvent compensatedEvent = TransactionEvent.builder()
                                .transactionId(event.transactionId())
                                .idempotencyKey(event.idempotencyKey())
                                .senderWalletId(event.senderWalletId())
                                .receiverWalletId(event.receiverWalletId())
                                .amount(event.amount())
                                .currency(event.currency())
                                .status(TransactionStatus.COMPENSATED.name())
                                .eventType("TRANSFER_SAGA_FAILED")
                                .timestamp(Instant.now())
                                .build();
                eventProducer.sendTransactionEvent(compensatedEvent);

                log.warn(
                                "Saga compensation committed: transactionId={}, sender re-credited amount={}. Transfer has been COMPENSATED.",
                                event.transactionId(), event.amount());
        }

        // ─── Private helpers ──────────────────────────────────────────────────────

        private TransactionEvent deserialize(ConsumerRecord<String, String> record) {
                try {
                        return objectMapper.readValue(record.value(), TransactionEvent.class);
                } catch (JsonProcessingException ex) {
                        log.error(
                                        "Saga event deserialization failed: topic={}, partition={}, offset={}, key={}",
                                        record.topic(), record.partition(), record.offset(), record.key(), ex);
                        // Wrap and throw — Kafka retry + DLT will handle further
                        throw new IllegalArgumentException(
                                        "Failed to deserialize saga event from partition=" + record.partition()
                                                        + ", offset=" + record.offset(),
                                        ex);
                }
        }
}
