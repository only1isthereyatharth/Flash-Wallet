# Wallet Core Service Summary

The **Wallet Core** is the heart of the Flash-Wallet ecosystem. It is responsible for managing financial state, executing P2P transfers, processing deposits, and ensuring high-concurrency data consistency. It implements sophisticated distributed locking (using Redisson) to prevent race conditions during concurrent transactions and enforces strict API idempotency (via AOP and Redis) to avoid double-charging users on network retries.

## Design Flow: Which File Acts When?

Here is the lifecycle of a complex transaction (e.g., a P2P Transfer) within the `wallet-core` service.

### Flow: Idempotent P2P Transfer Pipeline

```mermaid
sequenceDiagram
    autonumber
    participant Gateway as API Gateway
    participant Aspect as IdempotencyAspect
    participant Validator as @Validated Controller
    participant Controller as WalletController
    participant Service as WalletService
    participant LockMgr as LockManager
    participant DB as PostgreSQL (JPA)
    participant Kafka as Apache Kafka (wallet.saga.events)
    
    Gateway->>Aspect: 1. HTTP POST /transfer (with Idempotency-Key)
    activate Aspect
    Aspect->>Aspect: 2. Computes SHA-256 hash of request payload
    Aspect->>Aspect: 3. Checks Redis for existing processing state
    alt Cache Hit (Already Completed)
        Aspect->>Aspect: 3a. Verifies incoming payload hash matches cached hash
        note right of Aspect: Throws 422 on hash mismatch (replay attack)
        Aspect-->>Gateway: 3b. Returns cached successful JSON response
    else Cache Miss (New Request)
        Aspect->>Aspect: 3c. Sets state to PROCESSING in Redis with payload hash
    end
    
    Aspect->>Validator: 4. Forwards request to @Validated controller
    activate Validator
    Validator->>Validator: 5. Validates @CurrencyCode (ISO-4217), @Max amounts
    Validator->>Controller: 6. Forwards validated request
    deactivate Validator
    activate Controller
    Controller->>Service: 7. transfer(request, idempotencyKey)
    activate Service
    
    Service->>LockMgr: 8. executeWithLock(senderId)
    activate LockMgr
    note over LockMgr: Acquires single distributed lock on SENDER wallet only
    
    LockMgr->>Service: 9. executeTransferTx() [@Transactional Block]
    Service->>DB: 10. Load sender & receiver wallets, verify balance & currency
    Service->>Service: 11. Check overflow guard: balance + amount <= Long.MAX_VALUE
    Service->>DB: 12. Insert write-ahead Transaction record (status=INITIATED)
    Service->>DB: 13. Debit sender balance
    Service->>DB: 14. Update Transaction status to DEBIT_COMPLETED
    Service->>Kafka: 15. Publish TRANSFER_DEBIT_COMPLETED to wallet.saga.events
    note over Service,Kafka: Saga Step 2 (credit/compensate) handled async by TransferSagaConsumer
    
    LockMgr-->>Service: 16. @Transactional commits, LockMgr releases sender lock
    deactivate LockMgr
    Service-->>Controller: 17. Returns TransferResponse (status=DEBIT_COMPLETED)
    deactivate Service
    Controller-->>Aspect: 18. Returns ResponseEntity
    deactivate Controller
    
    Aspect->>Aspect: 19. Serializes response & sets state to COMPLETED in Redis (24h TTL)
    Aspect-->>Gateway: 20. Returns HTTP 202 Accepted (transfer in-flight, poll for final status)
    deactivate Aspect
```

Below is an exhaustive breakdown of every file within the `wallet-core` service and its exact purpose.

## 1. Controller Layer (`controller/`)
- **`WalletController.java`**: Exposes REST endpoints (`/api/v1/wallets`). Annotated with `@Validated` to enable method-level constraint validation. Methods like `/transfer` and `/deposit` are annotated with `@Idempotent` to trigger the idempotency aspect. It handles HTTP request mapping, passes `@Valid`-annotated Java `record` DTOs downstream, and returns strongly-typed responses (`TransferResponse`, `WalletResponse`, `TransactionStatusResponse`).


## 2. Idempotency Layer (`idempotency/`)
*Prevents users from being double-charged if their mobile app retries a transfer due to a network timeout. Binds idempotency keys to payload hashes to prevent replay attacks.*
- **`IdempotencyAspect.java`**: An AOP aspect that wraps methods annotated with `@Idempotent`. It intercepts the request, reads the `Idempotency-Key` header, computes SHA-256 hash of the request payload, and checks `IdempotencyService` (Redis). On a new request, it stores both the idempotency state and payload hash. On a retry, it verifies the incoming payload hash matches the stored hash; if they differ, it throws `IdempotencyPayloadMismatchException` (422 Unprocessable Entity) to prevent replay attacks with mutated payloads. If the hash matches and the request already succeeded, it short-circuits and returns the cached JSON response directly.
- **`IdempotencyService.java`**: Abstraction over Redis to store `IdempotencyState` with a Time-To-Live (TTL). Handles atomic state transitions (e.g., `tryStart`, `complete`, `fail`), now accepting `payloadHash` parameter to bind idempotency keys to specific request bodies.
- **`IdempotencyState.java`**: DTO representing the state of an idempotent request in Redis (status, cached HTTP response body, HTTP status code, and `payloadHash` for replay-attack prevention).
- **`Idempotent.java`**: A custom marker annotation used on controller methods that require idempotency guarantees.

## 3. Distributed Lock Layer (`lock/`)
*Prevents race conditions (e.g., spending the same $10 twice simultaneously) across multiple instances of the service.*
- **`LockManager.java`**: Uses Redisson (Redis) to acquire distributed locks on Wallet IDs. Provides `executeWithLock` (single lock, used by the saga-based transfer for sender-only locking and by deposits) and `executeWithDoubleLocks` (dual lock with ascending UUID ordering to prevent deadlocks, currently unused but available for direct-transfer scenarios).
- **`LockCallback.java`**: A functional interface representing the code block (like a database transaction) to be executed while holding the lock(s).

## 4. Service Layer (`service/`)
- **`WalletService.java`**: The core business logic orchestrator.
  - **Transfers (Saga Step 1)**: Calls `LockManager.executeWithLock()` on the sender wallet only (receiver is locked separately by the saga consumer in Step 2). Inside the lock, `executeTransferTx()` runs as a single `@Transactional` block: validates currencies (with `Locale.ROOT` for locale-safe normalization), checks balance, includes an arithmetic overflow guard (`if (balance > Long.MAX_VALUE - amount)`), inserts a write-ahead transaction record with `INITIATED` status, debits sender, updates status to `DEBIT_COMPLETED`, and publishes a `TRANSFER_DEBIT_COMPLETED` event to `wallet.saga.events`. The receiver credit (or compensating refund) is handled asynchronously by `TransferSagaConsumer`.
  - **Deposits**: Calls `LockManager.executeWithLock()` on the target wallet. Inside the lock, `executeDepositTx()` credits the wallet, includes an overflow guard before adding amount, records a `Transaction` with `INITIATED` → `COMPLETED` status, and publishes a `WALLET_DEPOSIT_COMPLETED` event to Kafka.
  - **Critical Design Choice**: Locks are acquired *outside* the JPA `@Transactional` boundary to prevent Hibernate dirty-read conflicts and database connection pool starvation. The `@Lazy` self-injection pattern is used to ensure Spring's transactional proxy is correctly applied on the internal `executeTransferTx` / `executeDepositTx` calls.

## 5. Event Producer Layer (`producer/`)
- **`WalletEventProducer.java`**: Uses `KafkaTemplate` to asynchronously publish `TransactionEvent` payloads to Kafka. It uses the `Transaction ID` as the Kafka routing key to ensure strict ordering of events per transaction. All event types published to `wallet.transaction.events` follow the `WALLET_*` prefix convention (`WALLET_TRANSFER_COMPLETED`, `WALLET_TRANSFER_SAGA_FAILED`, `WALLET_DEPOSIT_COMPLETED`) so the audit-worker contract stays consistent across producers.

## 6. Entity & Repository Layer (`model/` & `repository/`)
- **`Wallet.java`**: JPA entity mapping to the `wallets` table. Holds `balance` (BIGINT in lowest denomination), `currency` (VARCHAR 3-char ISO code), and a `@Version` field for Hibernate Optimistic Locking (secondary safety net beneath Redis locks). Uses explicit `@Getter`, `@Setter`, `@ToString`, `@EqualsAndHashCode(of = "id")` to fix the common JPA anti-pattern of `@Data` which incorrectly includes all lazy-loaded fields in equals/hashCode, causing issues with lazy-loaded collections and version fields.
- **`Transaction.java`**: JPA entity mapping to `transactions`. Records each operation with `idempotencyKey`, `senderWalletId`, `receiverWalletId`, `amount`, and a `TransactionStatus` enum (`INITIATED` → `DEBIT_COMPLETED` → `COMPLETED` / `COMPENSATED` / `FAILED`). `senderWalletId` is nullable for external deposit operations. Uses the same explicit Lombok annotations to ensure correct equals/hashCode behavior.
- **`TransactionStatus.java`**: Enum with values `INITIATED`, `DEBIT_COMPLETED`, `COMPLETED`, `COMPENSATED`, `FAILED`.
- **`WalletRepository.java` & `TransactionRepository.java`**: Spring Data JPA interfaces for database CRUD operations.

## 7. Exception Layer (`exception/`)
- **`GlobalExceptionHandler.java`**: A `@ControllerAdvice` class that catches and translates exceptions into clean, standardized JSON HTTP error responses:
  - Custom domain exceptions (`InsufficientBalanceException`, `WalletNotFoundException`, `LockAcquisitionException`, `IdempotencyConflictException`, `IdempotencyValidationException`) map to semantically appropriate HTTP status codes.
  - **`DataIntegrityViolationException`** from the database layer is caught and transformed: if the root cause mentions `idempotency_key` or `user_id` constraint violations, it returns 409 Conflict (duplicate wallet or idempotency key already processed).
  - **`IdempotencyPayloadMismatchException`** returns 422 Unprocessable Entity when a request is replayed with a different payload hash.
- **`IdempotencyPayloadMismatchException.java`**: Custom exception thrown when a retry of a completed request carries a different request payload, indicating a malicious replay attempt or client error.

## 8. Configuration Layer (`config/`)
- **`RedissonConfig.java`**: Sets up the RedissonClient connection pool to Redis for distributed locking.
- **`KafkaConfig.java`**: Defines the Kafka `NewTopic` (`wallet.transaction.events`) ensuring the topic exists with proper partitions and replicas on application startup.
- **`JacksonSecurityConfig.java`**: Explicitly configures the global Spring Boot `ObjectMapper` to fail on unknown properties and disable default typing configurations to prevent polymorphic deserialization attacks.
- **`OpenApiConfig.java`**: Configures Swagger/OpenAPI documentation generation for the REST API.
- **Configuration Source Policy (Current)**: For local runs and small-scale product use, runtime configuration is primarily managed in `application.yml` (plus environment overrides), while Java configuration classes may retain fallback defaults for minimal setup scenarios. If YAML binding is missing or unavailable, static defaults can take effect. Team rule: when changing service URLs, ports, Kafka broker/topic settings, or Redis endpoints, update both `application.yml` and related Java defaults to keep configuration consistent.

## 9. Data Transfer Objects (`dto/` & `event/`)
- **Input DTOs** (`CreateWalletRequest.java`, `DepositRequest.java`, `TransferRequest.java`): Immutable Java `record`s enforcing strict validation:
  - **Amount validation**: `@Max(1_000_000_000_000L)` prevents integer overflow attempts. Service layer adds secondary arithmetic overflow guards before performing `balance + amount` calculations.
  - **Currency validation**: Custom `@CurrencyCode` JSR-380 constraint validates 3-letter ISO-4217 currency codes via `java.util.Currency.getInstance()`, replacing naive `@Size(min=3,max=3)` checks. Normalizes to uppercase via `Locale.ROOT` for locale-safety.
- **Response DTOs** (`TransferResponse.java`, `WalletResponse.java`, `TransactionStatusResponse.java`): Immutable Java `record`s returned by the API. `WalletResponse` intentionally omits the Hibernate `version` field to avoid leaking internal entity state. **`TransactionStatusResponse`** (new) is a typed record (`record TransactionStatusResponse(UUID transactionId, String status)`) that replaces untyped `Map<String, String>` responses for transaction status polling.
- **Validation Annotations** (`CurrencyCode.java`, `CurrencyCodeValidator.java`): Custom JSR-380 constraint and validator for ISO-4217 currency codes, providing compile-time safety and clear error messages.
- **`TransactionEvent.java`**: An immutable Java `record` implementing `Serializable`. It is the Kafka payload model published to `wallet.transaction.events`. Fields include `transactionId`, `idempotencyKey`, `senderWalletId` (nullable for deposits), `receiverWalletId`, `amount`, `currency`, `status`, `eventType`, and `timestamp`.

---

## Phase 1 & 2 Hardening Improvements (Commit: bb16e8a)

### Phase 1: Data Integrity & Idempotency Hardening

**Goal**: Eliminate idempotency and concurrency vulnerabilities by binding idempotency keys to request payloads and fixing JPA entity design anti-patterns.

1. **Payload Hash Binding for Idempotency Keys**
   - **Changed files**: `IdempotencyAspect.java`, `IdempotencyService.java`, `IdempotencyState.java`
   - **What**: Idempotency keys are now bound to SHA-256 hashes of the request body. On retry, if the payload differs, a 422 response is returned instead of a cached response.
   - **Why**: Prevents replay attacks where a user might alter the amount and retry the same idempotency key.
   - **New exception**: `IdempotencyPayloadMismatchException.java` → 422 Unprocessable Entity.

2. **Database Constraint Violation Handling**
   - **Changed files**: `GlobalExceptionHandler.java`
   - **What**: `DataIntegrityViolationException` is now caught and inspected for constraint names. If `idempotency_key` or `user_id` constraints are violated, a 409 Conflict is returned instead of a generic 500.
   - **Why**: Gives clients proper feedback on duplicate wallet creation or idempotency key collisions.

3. **JPA Entity Anti-Pattern Fix**
   - **Changed files**: `Wallet.java`, `Transaction.java`
   - **What**: Replaced `@Data` with explicit `@Getter`, `@Setter`, `@ToString`, `@EqualsAndHashCode(of = "id")`.
   - **Why**: `@Data` incorrectly includes lazy-loaded relationships and versioning fields in equals/hashCode, breaking JPA identity semantics and causing Hibernate proxy comparison bugs.

### Phase 2: Input Validation Tightening

**Goal**: Enforce strict input validation at the API boundary to prevent arithmetic overflow, invalid currency codes, and type confusion.

1. **Arithmetic Overflow Prevention**
   - **Changed files**: `TransferRequest.java`, `DepositRequest.java`, `WalletService.java`, `TransferSagaConsumer.java`
   - **What**: 
     - DTOs now include `@Max(1_000_000_000_000L)` on amount fields.
     - Service methods add runtime overflow guards: `if (wallet.getBalance() > Long.MAX_VALUE - request.amount())` before arithmetic.
   - **Why**: Prevents integer wrapping attacks that could credit accounts beyond intended limits.

2. **ISO-4217 Currency Code Validation**
   - **Changed files**: `CurrencyCode.java` (NEW), `CurrencyCodeValidator.java` (NEW), `CreateWalletRequest.java`, `DepositRequest.java`, `TransferRequest.java`
   - **What**: Custom `@CurrencyCode` JSR-380 validator replaces naive `@Size(min=3,max=3)`. Validates codes via `java.util.Currency.getInstance()`.
   - **Why**: Rejects invalid/non-existent currency codes early and provides clear error messages.

3. **Locale-Safe Currency Normalization**
   - **Changed files**: `WalletService.java`
   - **What**: Changed `currency.toUpperCase()` to `currency.toUpperCase(Locale.ROOT)` in `createWallet()`.
   - **Why**: Prevents locale-specific uppercase rules (e.g., Turkish 'i' → 'İ') from causing currency matching bugs.

4. **Type-Safe Response DTOs**
   - **Changed files**: `TransactionStatusResponse.java` (NEW), `WalletController.java`
   - **What**: New `TransactionStatusResponse` record replaces `Map<String, String>` in transaction status polling endpoint.
   - **Why**: Provides compile-time type safety and prevents accidental field leakage.

5. **Controller Validation Activation**
   - **Changed files**: `WalletController.java`
   - **What**: Added `@Validated` class-level annotation to activate method-level constraint validation.
   - **Why**: Ensures JSR-380 violations are caught at the controller boundary, not buried in service logic.

6. **Removal of Output Sanitization**
   - **Deleted**: `SanitizationAspect.java`
   - **Why**: HTML-escaping on the service layer corrupts stored data for non-HTML consumers (e.g., mobile apps, other microservices). Sanitization should occur at the presentation layer (API Gateway / frontend), not in the core service.

---

## Redis Design Flow: Idempotency & Distributed Locking

Redis plays **two separate roles** inside wallet-core. Each role uses its own key namespace and its own Redis client so they never interfere with each other.

---

### Role 1 — Idempotency (stop double-charging on retries)

> **The problem it solves**: A mobile app times out and retries a transfer. Without this, the user gets charged twice.  
> **The extra protection**: The request body is hashed (SHA-256) and stored alongside the key. If someone retries with a different amount but the same key, it is rejected — preventing replay attacks.

**Redis key**: `idempotency:{idempotencyKey}`  
**Owner**: `IdempotencyAspect` + `IdempotencyService`

#### What happens step by step

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Aspect as IdempotencyAspect
    participant Redis
    participant Service as WalletService

    Client->>Aspect: POST /transfer  (Idempotency-Key + payload)
    Aspect->>Aspect: Hash the request body with SHA-256
    Aspect->>Redis: Does this key already exist?  (GET idempotency:KEY)

    alt Brand new request (key does not exist)
        Aspect->>Redis: Save key as PROCESSING + hash, expires in 5 min  (SETNX + EXPIRE)
        Aspect->>Service: Run the transfer
        Service-->>Aspect: Success
        Aspect->>Redis: Update key to COMPLETED + hash + cached response, expires in 24 h  (SET + EXPIRE)
        Aspect-->>Client: 202 Accepted
    else Another request with the same key is still running
        Aspect-->>Client: 409 Conflict — wait and retry
    else Key is COMPLETED and payload hash matches
        Aspect-->>Client: Return the cached response instantly (no database hit)
    else Key is COMPLETED but payload hash is DIFFERENT
        Aspect-->>Client: 422 Unprocessable Entity — payload was changed, rejected
    end
```

#### What is stored in Redis for each key

| Field | Example value | Redis command used | Why it is needed |
|-------|---------------|-------------------|------------------|
| `status` | `PROCESSING` / `COMPLETED` | `SETNX` (set-if-not-exists) on first write; `SET` on completion | Tracks whether the request finished |
| `payloadHash` | SHA-256 hex | Stored as a hash field alongside status | Detects if the retry changed the request body |
| `responseBody` | Serialized JSON | Stored on `complete()` via `SET` | Returned directly on a safe retry — no DB call |
| `httpStatus` | `202` | Stored on `complete()` via `SET` | HTTP status to replay back to the client |
| TTL | 5 min / 24 h | `EXPIRE` (auto-set on each write) | 5-min guard prevents orphaned keys if the service crashes |

---

### Role 2 — Distributed wallet locks (stop two requests spending the same money)

> **The problem it solves**: Two concurrent transfers from the same wallet could both read the same balance, both think there is enough money, and both debit — resulting in overspending.  
> **How it is solved**: Before touching the database, a Redis lock is placed on the wallet ID. Only one request can hold the lock at a time.

**Redis key**: `lock:wallet:{walletId}`  
**Owner**: `LockManager` using the Redisson library

#### Why the lock wraps the database transaction (not the other way around)

The lock is acquired *before* `@Transactional` starts and released *after* the commit. If the lock were inside the transaction, a second thread could grab it before Hibernate flushes — creating a dirty read. Keeping it outside also means the database connection is only held for the short write window, not the full lock duration.

#### What happens step by step

```mermaid
sequenceDiagram
    autonumber
    participant Service as WalletService
    participant Lock as LockManager (Redisson)
    participant Redis
    participant DB as PostgreSQL

    Service->>Lock: executeWithLock(senderWalletId)
    Lock->>Redis: Try to claim lock  (SET lock:wallet:SENDER NX PX leaseTimeMs)
    Note right of Redis: NX = only if not exists, PX = auto-expire in ms

    alt Lock is free (SET returned OK)
        Redis-->>Lock: Claimed
        Lock->>DB: Open @Transactional — debit sender, update status
        DB-->>Lock: Committed
        Lock->>Redis: Release lock  (DEL lock:wallet:SENDER)
        Lock-->>Service: Done
    else Lock is already held (SET returned nil)
        Redis-->>Lock: Rejected
        Lock-->>Service: LockAcquisitionException — caller gets 409
    end
```

#### Lock safety guarantees

| Scenario | What happens | Redis command |
|----------|--------------|---------------|
| Normal completion | Lock is released immediately after the DB commit | `DEL lock:wallet:{id}` |
| Service crashes mid-transaction | Lock expires automatically via Redisson lease timeout — no deadlock | `PX` (TTL set during `SET NX`) auto-expires the key |
| Two requests on the same wallet | Second one is rejected while the first holds the lock | `SET NX` returns `nil` → `LockAcquisitionException` |
| Two requests on different wallets | Both run in parallel — no blocking between unrelated wallets | Different keys, no contention |

---

### Redis Key Namespace Summary

| Key pattern | Role | Redis commands used | TTL | What it does in plain language |
|-------------|------|---------------------|-----|--------------------------------|
| `idempotency:{key}` | Idempotency | `GET`, `SETNX`, `SET`, `EXPIRE`, `DEL` | 5 min (in-flight) / 24 h (done) | Remember what happened so retries are safe |
| `lock:wallet:{walletId}` | Distributed lock | `SET NX PX`, `DEL` (via Redisson) | Short lease, auto-expires on crash | One request at a time per wallet |
| `request_rate_limiter.{clientKey}.*` | API Gateway (`RedisRateLimiter`) | Auto-renewed | Token-bucket rate limiting (see api-gateway-summary) |

---

## Build & Deployment Notes

- **Java**: Requires JDK 21+ (records, pattern matching)
- **Maven**: Multi-module build. Build wallet-core with `mvn clean install` from `flash-wallet/` directory.
- **Database**: Runs schema migrations on `wallet_core_db` PostgreSQL database.
- **Redis**: Required for distributed locking and idempotency caching.
- **Kafka**: Requires `wallet.transaction.events` topic (auto-created by `KafkaConfig`).
- **Docker**: Provided in [docker-compose.yml](../docker-compose.yml) at project root.
