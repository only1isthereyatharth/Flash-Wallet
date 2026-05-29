# Modernization Plan: Spring Boot 4.x Upgrade

## Plan Overview

| Field | Value |
|-------|-------|
| **Plan Name** | Spring Boot 4.x Upgrade |
| **Project** | Flash-Wallet (multi-module Maven) |
| **Language** | Java 21 |
| **Created** | 2026-05-29 |
| **Status** | Draft |

## Current State

| Component | Current Version |
|-----------|----------------|
| Spring Boot | 3.2.5 (spring-boot-starter-parent) |
| Java | 21 (parent), 17 (wallet-core & audit-worker compiler overrides) |
| Spring Cloud | 2023.0.3 (api-gateway) |
| Spring Framework | 6.1.x (managed by Spring Boot 3.2.5) |
| Redisson | 3.43.0 |
| springdoc-openapi | 2.5.0 (wallet-core) |
| Lombok | 1.18.46 |
| maven-compiler-plugin | 3.13.0 |
| Jib Maven Plugin | 3.4.0 |
| Dockerfiles | eclipse-temurin:21-jre-alpine |

## Target State

| Component | Target Version |
|-----------|---------------|
| Spring Boot | 4.0.x (latest GA) |
| Java | 21 (consistent across all modules) |
| Spring Cloud | 2025.0.x (Spring Boot 4.x compatible release train) |
| Spring Framework | 7.0.x (managed by Spring Boot 4.0.x) |
| Redisson | Latest compatible with Spring Boot 4.x |
| springdoc-openapi | 2.8.x+ (latest compatible) |
| Lombok | Latest (1.18.x) |
| maven-compiler-plugin | 3.14.x (latest) |
| Jib Maven Plugin | 3.4.x (latest) |
| Dockerfiles | eclipse-temurin:21-jre-alpine (unchanged, already on 21) |

## Modules Affected

| Module | Key Dependencies | Files to Update |
|--------|-----------------|-----------------|
| **flash-wallet (parent)** | spring-boot-starter-parent, maven-compiler-plugin, Lombok, Redisson managed version | `pom.xml` |
| **api-gateway** | spring-cloud-starter-gateway, spring-boot-starter-data-redis-reactive | `api-gateway/pom.xml`, `api-gateway/src/main/resources/application.yml` |
| **wallet-core** | spring-boot-starter-web, spring-boot-starter-data-jpa, spring-kafka, Redisson, springdoc-openapi | `wallet-core/pom.xml`, `wallet-core/src/main/resources/application.yml` |
| **audit-worker** | spring-boot-starter, spring-boot-starter-data-jpa, spring-kafka | `audit-worker/pom.xml`, `audit-worker/src/main/resources/application.yml` |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Spring Boot 4.x breaking changes (removed deprecated APIs, config property renames) | High | Review Spring Boot 4.0 migration guide; test each module after changes |
| Spring Cloud Gateway API changes for 2025.0.x | Medium | Consult Spring Cloud Gateway release notes for breaking changes |
| Redisson compatibility with Spring Boot 4.x autoconfiguration | Medium | Verify Redisson Spring Boot starter supports Spring Boot 4.x |
| Hibernate 7.x changes (via Spring Boot 4.x) | Medium | Review JPA dialect and property changes; `spring.jpa.database-platform` may be deprecated |
| springdoc-openapi compatibility with Spring Framework 7.x | Low | springdoc actively tracks Spring Boot releases |

## Task Execution Order

Tasks are ordered to minimize cascading build failures:

1. **001** — Fix compiler version consistency (prerequisite, no dependency on other tasks)
2. **002** — Upgrade Spring Boot to 4.x (core upgrade, all other tasks depend on this)
3. **003** — Upgrade Spring Cloud (depends on 002)
4. **004** — Upgrade springdoc-openapi (depends on 002)
5. **005** — Upgrade Redisson (depends on 002)
6. **006** — Upgrade Lombok and maven-compiler-plugin (depends on 002)
7. **007** — Handle Spring Boot 4.x breaking changes (depends on 002–006)
8. **008** — Update Dockerfiles and build plugins (depends on all above)
9. **009** — Build verification (final gate, depends on all above)
