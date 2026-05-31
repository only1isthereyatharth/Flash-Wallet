# Configuration Guide — Flash Wallet

## Overview

Each microservice's configuration is managed through a clean, unified config layout:

```
<module>/src/main/resources/
├── application.yml          ← Common config (Infra & Business, clearly sectioned)
└── application-local.yml    ← Local environment overrides (Only what's different)
```

No bootstrap files or separate files are used. Everything is consolidated inside `application.yml` and sectioned with explicit headers.

---

## Why This Structure?

1. **Simplicity & Discoverability**: Having configs in a single `application.yml` file is standard and practical. Developers don't need to jump between multiple files to understand how a service is configured.
2. **Infra vs. App Separation**: Using structured visual comment blocks in the YAML file distinguishes environment-specific settings (databases, ports, credentials) from app/business configuration logic (saga retry rules, rate limits, CORS policies).
3. **No Drift / Redundancy**: Environment variables and system properties use fallback defaults (e.g. `${DB_HOST:localhost}`) inside the common configuration. Only overrides specific to local machine contexts (such as DEBUG logging) are isolated to `application-local.yml`.
4. **Dev vs. Prod Control**: In production settings, environment-driven configurations are set via container environment variables, while local machine overrides are isolated in `application-local.yml`.

---

## File Sections inside `application.yml`

Inside each service's `application.yml`, config properties are divided into two blocks:

### 1. Infrastructure Config (Environment-Driven)
*Properties that change based on the deployment target, cloud resources, credentials, or logging levels.*
- **Databases & Pools**: PostgreSQL, Redisson URL, Hikari pool sizes
- **Brokers**: Kafka bootstrap servers, key/value serializers
- **Endpoints & Gateway Routing**: Actuator probes, upstream service targets (e.g. `wallet-core-uri`)
- **Logging**: Level properties for system/framework packages

### 2. Application Config (Business-Driven)
*Properties that dictate the functional/domain rules of the application, API contracts, and resilience policies.*
- **Contracts**: Jackson serialization rules (`fail-on-unknown-properties`)
- **Resilience Policy**: Resilience4j sliding window size, thresholds, wait durations
- **Sagas & Topics**: Saga step limits, retry counts, listener concurrency, target topic names
- **Access Policies**: Gateway CORS rules, strict UUID validation configs, rate-limiting limits

---

## The Local Development Profile (`application-local.yml`)

The `application-local.yml` file contains overrides specific to local development.
- **Rule**: **Only override what is different** from the default settings defined in `application.yml`.
- Do not duplicate configurations that are already covered by defaults (e.g., if a database host already defaults to `localhost` in `application.yml`).
- Typical use cases include activating DEBUG logging on a service, changing timeout settings locally, or adjusting pool sizes.

### Activating the Local Profile
To run a microservice with your local configuration active:
```bash
# Via maven wrapper
./mvnw spring-boot:run -Dspring-boot.run.profiles=local

# Or running the packaged jar directly
java -jar target/service-name.jar --spring.profiles.active=local
```

---

## Per-Service Layout Reference

### wallet-core
- **INFRA**: PostgreSQL datasource (url, credentials, pool), JPA/Hibernate dialect & ddl mode, Redisson URL, Kafka broker connections, default logging levels.
- **APP**: Jackson deserialization behavior, Transfer Saga configurations (retry count, intervals, listener concurrency, event topic names).

### api-gateway
- **INFRA**: Spring Cloud Gateway routing client timeouts, microservice destination URIs (`wallet-core-uri`), actuator probes & prometheus metric endpoints, gateway logging configurations, HSTS rules.
- **APP**: Gateway CORS allowed origins/methods/headers, strict idempotency validation configurations, API request rate limiter values (replenish rate, burst capacity), Resilience4j circuit-breaker instances.

### audit-worker
- **INFRA**: Audit DB target configs & Hikari pools, JPA configuration, Kafka consumer group/broker/serializer configurations.
- **APP**: Audit event topics, dead-letter topic parameters, worker retry policies, consumer concurrency.
