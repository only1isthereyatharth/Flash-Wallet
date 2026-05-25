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
    participant Controller as WalletController
    participant Service as WalletService
    participant LockMgr as LockManager
    participant DB as PostgreSQL (JPA)
    participant Producer as WalletEventProducer
    participant Kafka as Apache Kafka Broker
    
    Gateway->>Aspect: 1. HTTP POST /transfer (with Idempotency-Key)
    activate Aspect
    Aspect->>Aspect: 2. Checks Redis for existing processing state
    alt Cache Hit (Already Completed)
        Aspect-->>Gateway: 2a. Returns cached successful JSON response
    else Cache Miss (New Request)
        Aspect->>Aspect: 2b. Sets state to PROCESSING in Redis
    end
    
    Aspect->>Controller: 3. Forwards request
    activate Controller
    Controller->>Service: 4. transfer(request, idempotencyKey)
    activate Service
    
    Service->>LockMgr: 5. executeWithDoubleLocks(senderId, receiverId)
    activate LockMgr
    note over LockMgr: Sorts UUIDs alphabetically to prevent deadlocks
    LockMgr->>LockMgr: 6. Acquires Distributed Locks via Redisson
    
    LockMgr->>Service: 7. executeTransferTx() [Transactional Block]
    Service->>DB: 8. Load & Lock Wallets, verify balances
    Service->>DB: 9. Deduct sender, credit receiver, save Transaction
    Service->>Producer: 10. sendTransactionEvent(event)
    activate Producer
    Producer->>Kafka: 11. Async publish to 'wallet.transaction.events'
    deactivate Producer
    
    LockMgr-->>Service: 12. Transaction commits, LockMgr releases locks
    deactivate LockMgr
    Service-->>Controller: 13. Returns TransferResponse
    deactivate Service
    Controller-->>Aspect: 14. Returns ResponseEntity
    deactivate Controller
    
    Aspect->>Aspect: 15. Serializes response & sets state to COMPLETED in Redis
    Aspect-->>Gateway: 16. Returns final HTTP Response
    deactivate Aspect
```

Below is an exhaustive breakdown of every file within the `wallet-core` service and its exact purpose.

## 1. Controller Layer (`controller/`)
- **`WalletController.java`**: Exposes REST endpoints (`/api/v1/wallets`). Methods like `/transfer` and `/deposit` are annotated with `@Idempotent` to trigger the idempotency aspect. It handles HTTP request mapping, input validation, and forwards requests to the `WalletService`.

## 2. Idempotency Layer (`idempotency/`)
*Prevents users from being double-charged if their mobile app retries a transfer due to a network timeout.*
- **`IdempotencyAspect.java`**: An AOP aspect that wraps methods annotated with `@Idempotent`. It intercepts the request, reads the `Idempotency-Key` header, and checks `IdempotencyService` (Redis). If it's a new request, it lets it proceed and caches the JSON response upon success. If it's a retry of an already completed request, it short-circuits and returns the cached JSON response directly, bypassing the controller entirely.
- **`IdempotencyService.java`**: Abstraction over Redis to store `IdempotencyState` with a Time-To-Live (TTL). Handles atomic state transitions (e.g., `tryStart`, `complete`, `fail`).
- **`IdempotencyState.java`**: DTO representing the state of an idempotent request in Redis (status, cached HTTP response body, HTTP status code).
- **`Idempotent.java`**: A custom marker annotation used on controller methods that require idempotency guarantees.

## 3. Distributed Lock Layer (`lock/`)
*Prevents race conditions (e.g., spending the same $10 twice simultaneously) across multiple instances of the service.*
- **`LockManager.java`**: Uses Redisson (Redis) to acquire distributed locks on Wallet IDs. It features `executeWithDoubleLocks` which smartly sorts the sender and receiver UUIDs alphabetically before locking them to categorically prevent distributed deadlocks between concurrent bidirectional transfers.
- **`LockCallback.java`**: A functional interface representing the code block (like a database transaction) to be executed while holding the lock(s).

## 4. Service Layer (`service/`)
- **`WalletService.java`**: The core business logic orchestrator. 
  - For transfers, it calls `LockManager` to secure both wallets, then delegates to `executeTransferTx()` to perform the JPA transaction (balance checks, deductions).
  - **Critical Design Choice:** It manages the transaction boundary explicitly—locks are acquired *outside* the JPA `@Transactional` block to prevent dirty reads and Hibernate optimistic locking conflicts.
  - After saving to the DB, it calls `WalletEventProducer` to stream the transaction event to Kafka for the `audit-worker` to consume.

## 5. Event Producer Layer (`producer/`)
- **`WalletEventProducer.java`**: Uses `KafkaTemplate` to asynchronously publish `TransactionEvent` payloads to Kafka. It uses the `Transaction ID` as the Kafka routing key to ensure strict ordering of events per transaction.

## 6. Entity & Repository Layer (`model/` & `repository/`)
- **`Wallet.java` & `Transaction.java`**: JPA entities mapping to the database tables. `Wallet` includes a `@Version` field for Optimistic Locking (a secondary safety net beneath the Redis distributed locks to protect against split-brain scenarios).
- **`WalletRepository.java` & `TransactionRepository.java`**: Spring Data JPA interfaces for database CRUD operations.

## 7. Exception Layer (`exception/`)
- **`GlobalExceptionHandler.java`**: A `@ControllerAdvice` class that catches custom exceptions (like `InsufficientBalanceException`, `WalletNotFoundException`, `LockAcquisitionException`, or `IdempotencyConflictException`) and translates them into clean, standardized JSON HTTP error responses for the API Gateway.
- **`InsufficientBalanceException.java`, `WalletNotFoundException.java`, `LockAcquisitionException.java`, `IdempotencyConflictException.java`, `IdempotencyValidationException.java`**: Custom business exceptions representing specific failure states in the domain logic.

## 8. Configuration Layer (`config/`)
- **`RedissonConfig.java`**: Sets up the RedissonClient connection pool to Redis for distributed locking.
- **`KafkaConfig.java`**: Defines the Kafka `NewTopic` (`wallet.transaction.events`) ensuring the topic exists with proper partitions and replicas on application startup.
- **`OpenApiConfig.java`**: Configures Swagger/OpenAPI documentation generation for the REST API.

## 9. Data Transfer Objects (`dto/` & `event/`)
- **`CreateWalletRequest.java`, `DepositRequest.java`, `TransferRequest.java`, `TransferResponse.java`, `WalletResponse.java`**: POJOs used for HTTP request/response serialization with Jackson, including `jakarta.validation` annotations (like `@Valid`, `@NotNull`) to enforce strict API contracts.
- **`TransactionEvent.java`**: The payload model serialized into JSON and sent to Kafka for downstream consumers (like the `audit-worker`).
