package com.services.wallet.service;

import com.services.wallet.dto.DepositRequest;
import com.services.wallet.dto.TransferRequest;
import com.services.wallet.dto.TransferResponse;
import com.services.wallet.dto.WalletResponse;
import com.services.wallet.config.KafkaConfig;
import com.services.wallet.event.TransactionEvent;
import com.services.wallet.exception.InsufficientBalanceException;
import com.services.wallet.exception.WalletNotFoundException;
import com.services.wallet.idempotency.IdempotencyService;
import com.services.wallet.lock.LockManager;
import com.services.wallet.model.Transaction;
import com.services.wallet.model.TransactionStatus;
import com.services.wallet.model.Wallet;
import com.services.wallet.producer.WalletEventProducer;
import com.services.wallet.repository.TransactionRepository;
import com.services.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class WalletService {

    @Value("${wallet.transfer.mode:async}")
    private String transferMode;

    private final WalletRepository walletRepository;
    private final TransactionRepository transactionRepository;
    private final LockManager lockManager;
    private final WalletEventProducer eventProducer;
    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    private final IdempotencyService idempotencyService;

    @Autowired
    @Lazy
    private WalletService self;

    /**
     * Executes a P2P transfer between two wallets.
     * Guarantees locks are acquired outside of the JPA transaction boundary.
     */
    public TransferResponse transfer(TransferRequest request, String idempotencyKey) {
        if ("sync".equalsIgnoreCase(transferMode)) {
            return transferSync(request, idempotencyKey);
        }
        try {
            // Try the standard async Saga route
            return transferAsync(request, idempotencyKey);
        } catch (Exception e) {
            // If Kafka is down, or Saga initiation fails:
            log.error("Saga flow failed (possibly Kafka outage). Falling back to synchronous double-lock pathway...",
                    e);

            // 1. Reset/fail the idempotency key state in Redis so the retry can run
            idempotencyService.fail(idempotencyKey);

            // 2. Fallback to synchronous double-lock execution
            return transferSync(request, idempotencyKey);
        }
    }

    public TransferResponse transferAsync(TransferRequest request, String idempotencyKey) {
        log.info("Initiating P2P transfer from wallet {} to wallet {} of amount {}",
                request.senderWalletId(), request.receiverWalletId(), request.amount());
        try {
            return lockManager.executeWithLock(
                    request.senderWalletId(),
                    5000, // 5 seconds max wait to acquire locks
                    10000, // 10 seconds auto-lease guard
                    () -> self.executeTransferTx(request, idempotencyKey));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to execute transfer under distributed lock", e);
            throw new RuntimeException("Transaction failed due to lock/database issues", e);
        }
    }

    public TransferResponse transferSync(TransferRequest request, String idempotencyKey) {
        log.warn("KAFKA DOWN DETECTED OR CONFIG SWITCHED: Executing sync transfer with double locks");

        try {
            // Lock both sender and receiver wallets for atomic sync transfer
            return lockManager.executeWithDoubleLocks(
                    request.senderWalletId(),
                    request.receiverWalletId(),
                    5000,
                    10_000,
                    () -> self.executeSyncTransferTx(request, idempotencyKey));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Sync transfer failed under double lock", e);
        }
    }

    /**
     * Executes a deposit into a single wallet.
     */
    public WalletResponse deposit(DepositRequest request, String idempotencyKey) {
        log.info("Initiating deposit to wallet {} of amount {}", request.walletId(), request.amount());
        try {
            return lockManager.executeWithLock(
                    request.walletId(),
                    5000,
                    10000,
                    () -> self.executeDepositTx(request, idempotencyKey));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to execute deposit under distributed lock", e);
            throw new RuntimeException("Transaction failed due to lock/database issues", e);
        }
    }

    /**
     * Saga Step 1: validates the transfer request, debits the sender, and
     * records the transaction as {@code DEBIT_COMPLETED}.
     *
     * <p>
     * A write-ahead record with status {@code INITIATED} is persisted before
     * any balance mutation. If the debit succeeds, the status is atomically
     * updated to {@code DEBIT_COMPLETED} in the same {@code @Transactional} commit.
     *
     * <p>
     * This method publishes a {@code TRANSFER_DEBIT_COMPLETED} event to
     * {@code wallet.saga.events}. The
     * {@link com.services.wallet.saga.TransferSagaConsumer}
     * picks this up and performs Step 2 (receiver credit) or the compensating
     * transaction (sender re-credit) if the credit fails.
     *
     * <p>
     * The response status is {@code "DEBIT_COMPLETED"} — the transfer is not yet
     * complete.
     * Callers should poll {@code GET /api/v1/wallets/transactions/{transactionId}}
     * to determine the final outcome.
     */
    @Transactional
    public TransferResponse executeTransferTx(TransferRequest request, String idempotencyKey) {
        // Load wallets
        Wallet sender = walletRepository.findById(request.senderWalletId())
                .orElseThrow(() -> new WalletNotFoundException("Sender wallet not found: " + request.senderWalletId()));
        Wallet receiver = walletRepository.findById(request.receiverWalletId())
                .orElseThrow(
                        () -> new WalletNotFoundException("Receiver wallet not found: " + request.receiverWalletId()));

        // Validate currencies match request
        if (!sender.getCurrency().equalsIgnoreCase(request.currency()) ||
                !receiver.getCurrency().equalsIgnoreCase(request.currency())) {
            throw new IllegalArgumentException(
                    "Currency mismatch between wallets and request: expected " + request.currency());
        }

        // Check sufficient funds
        if (sender.getBalance() < request.amount()) {
            log.warn("Transfer failed: Insufficient balance in sender wallet. Balance={}, Attempted={}",
                    sender.getBalance(), request.amount());
            throw new InsufficientBalanceException("Insufficient balance in wallet: " + sender.getId());
        }

        // 1. Persist write-ahead Transaction with status INITIATED
        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID())
                .idempotencyKey(idempotencyKey)
                .senderWalletId(sender.getId())
                .receiverWalletId(receiver.getId())
                .amount(request.amount())
                .status(TransactionStatus.INITIATED)
                .build();
        transactionRepository.save(transaction);

        // 2. Deduct from sender
        sender.setBalance(sender.getBalance() - request.amount());
        walletRepository.save(sender);

        // 3. Update transaction to DEBIT_COMPLETED
        transaction.setStatus(TransactionStatus.DEBIT_COMPLETED);
        transactionRepository.save(transaction);

        // ── Publish to saga coordination topic ───────────────────────────────
        // The TransferSagaConsumer will pick this up and execute Step 2 (credit)
        // or the compensating transaction (re-credit sender) if credit fails.
        TransactionEvent sagaEvent = TransactionEvent.builder()
                .transactionId(transaction.getId())
                .idempotencyKey(idempotencyKey)
                .senderWalletId(sender.getId())
                .receiverWalletId(receiver.getId())
                .amount(request.amount())
                .currency(sender.getCurrency())
                .status(TransactionStatus.DEBIT_COMPLETED.name())
                .eventType("TRANSFER_DEBIT_COMPLETED")
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(KafkaConfig.SAGA_EVENTS_TOPIC, transaction.getId().toString(), sagaEvent);

        log.info("Saga Step 1 committed: transactionId={}, senderWalletId={}, amount={}, status=DEBIT_COMPLETED",
                transaction.getId(), sender.getId(), request.amount());

        return TransferResponse.builder()
                .transactionId(transaction.getId())
                .senderWalletId(sender.getId())
                .receiverWalletId(receiver.getId())
                .amount(request.amount())
                .status("DEBIT_COMPLETED")
                .message("Transfer initiated. Use the transactionId to poll for the final status.")
                .build();
    }

    /**
     * Transactional block for deposits.
     */
    @Transactional
    public WalletResponse executeDepositTx(DepositRequest request, String idempotencyKey) {
        Wallet wallet = walletRepository.findById(request.walletId())
                .orElseThrow(() -> new WalletNotFoundException("Wallet not found: " + request.walletId()));

        if (!wallet.getCurrency().equalsIgnoreCase(request.currency())) {
            throw new IllegalArgumentException("Currency mismatch for deposit: expected " + wallet.getCurrency());
        }

        // ── Record transaction as INITIATED (write-ahead) ────────────────────
        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID())
                .idempotencyKey(idempotencyKey)
                .senderWalletId(null) // null represents external ledger system deposit
                .receiverWalletId(wallet.getId())
                .amount(request.amount())
                .status(TransactionStatus.INITIATED)
                .build();
        transactionRepository.save(transaction);

        // Overflow guard: ensure balance + amount doesn't exceed Long.MAX_VALUE
        if (wallet.getBalance() > Long.MAX_VALUE - request.amount()) {
            throw new IllegalArgumentException("Deposit would overflow wallet balance");
        }

        // Update balance
        wallet.setBalance(wallet.getBalance() + request.amount());
        walletRepository.save(wallet);

        // ── Update transaction to COMPLETED ──────────────────────────────────
        transaction.setStatus(TransactionStatus.COMPLETED);
        transactionRepository.save(transaction);

        // Stream transaction event to Kafka
        TransactionEvent event = TransactionEvent.builder()
                .transactionId(transaction.getId())
                .idempotencyKey(idempotencyKey)
                .senderWalletId(null)
                .receiverWalletId(wallet.getId())
                .amount(request.amount())
                .currency(wallet.getCurrency())
                .status(TransactionStatus.COMPLETED.name())
                .eventType("WALLET_DEPOSIT_COMPLETED")
                .timestamp(Instant.now())
                .build();
        eventProducer.sendTransactionEvent(event);

        log.info("Deposit transaction committed successfully: ID={}", transaction.getId());

        return mapToResponse(wallet);
    }

    @Transactional
    public TransferResponse executeSyncTransferTx(TransferRequest request, String idempotencyKey) {
        Wallet sender = walletRepository.findById(request.senderWalletId())
                .orElseThrow(() -> new WalletNotFoundException("Sender wallet not found: " + request.senderWalletId()));
        Wallet receiver = walletRepository.findById(request.receiverWalletId())
                .orElseThrow(
                        () -> new WalletNotFoundException("Receiver wallet not found: " + request.receiverWalletId()));

        if (!sender.getCurrency().equalsIgnoreCase(request.currency()) ||
                !receiver.getCurrency().equalsIgnoreCase(request.currency())) {
            throw new IllegalArgumentException("Currency mismatch");
        }

        if (sender.getBalance() < request.amount()) {
            throw new InsufficientBalanceException("Insufficient funds in sender wallet");
        }

        if (receiver.getBalance() > Long.MAX_VALUE - request.amount()) {
            throw new IllegalArgumentException("Receiver balance overflow the guard");
        }

        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID())
                .idempotencyKey(idempotencyKey)
                .senderWalletId(sender.getId())
                .receiverWalletId(receiver.getId())
                .amount(request.amount())
                .status(TransactionStatus.COMPLETED)
                .build();

        transactionRepository.save(transaction);
        sender.setBalance(sender.getBalance() - request.amount());
        receiver.setBalance(receiver.getBalance() + request.amount());
        walletRepository.save(sender);
        walletRepository.save(receiver);

        return TransferResponse.builder()
                .transactionId(transaction.getId())
                .senderWalletId(sender.getId())
                .receiverWalletId(receiver.getId())
                .amount(request.amount())
                .status("COMPLETED")
                .message("Synchronous transfer completed successfully.")
                .build();
    }

    @Transactional
    public WalletResponse createWallet(UUID userId, String currency) {
        if (walletRepository.findByUserId(userId).isPresent()) {
            throw new IllegalArgumentException("Wallet already exists for user: " + userId);
        }

        Wallet wallet = Wallet.builder()
                .id(UUID.randomUUID())
                .userId(userId)
                .balance(0L)
                .currency(currency.toUpperCase(Locale.ROOT))
                .build();

        walletRepository.save(wallet);
        log.info("Created new wallet: ID={}, UserID={}", wallet.getId(), userId);
        return mapToResponse(wallet);
    }

    @Transactional(readOnly = true)
    public WalletResponse getWallet(UUID walletId) {
        Wallet wallet = walletRepository.findById(walletId)
                .orElseThrow(() -> new WalletNotFoundException("Wallet not found with ID: " + walletId));
        return mapToResponse(wallet);
    }

    @Transactional(readOnly = true)
    public WalletResponse getWalletByUserId(UUID userId) {
        Wallet wallet = walletRepository.findByUserId(userId)
                .orElseThrow(() -> new WalletNotFoundException("Wallet not found for user: " + userId));
        return mapToResponse(wallet);
    }

    private WalletResponse mapToResponse(Wallet wallet) {
        return WalletResponse.builder()
                .id(wallet.getId())
                .userId(wallet.getUserId())
                .balance(wallet.getBalance())
                .currency(wallet.getCurrency())
                .updatedAt(wallet.getUpdatedAt())
                .build();
    }
}
