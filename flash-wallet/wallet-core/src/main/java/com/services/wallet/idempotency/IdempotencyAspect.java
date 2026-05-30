package com.services.wallet.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.wallet.exception.IdempotencyConflictException;
import com.services.wallet.exception.IdempotencyPayloadMismatchException;
import com.services.wallet.exception.IdempotencyValidationException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class IdempotencyAspect {

    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @Around("@annotation(com.services.wallet.idempotency.Idempotent)")
    public Object handleIdempotency(ProceedingJoinPoint joinPoint) throws Throwable {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return joinPoint.proceed();
        }

        HttpServletRequest request = attributes.getRequest();
        String idempotencyKey = request.getHeader("Idempotency-Key");

        if (idempotencyKey == null || idempotencyKey.trim().isEmpty()) {
            log.warn("Request missing required Idempotency-Key header");
            throw new IdempotencyValidationException("Required header 'Idempotency-Key' is missing or empty.");
        }

        // Compute SHA-256 of the request body DTO to bind the key to the payload
        String payloadHash = computePayloadHash(joinPoint);

        log.info("Processing idempotent request with key: {}", idempotencyKey);

        // Try to start processing. Guard TTL set to 5 minutes to release keys if service crashes mid-flight.
        boolean started = idempotencyService.tryStart(idempotencyKey, Duration.ofMinutes(5), payloadHash);

        if (!started) {
            // Key already exists. Fetch the state from Redis.
            IdempotencyState state = idempotencyService.get(idempotencyKey);
            if (state == null) {
                // Edge case: state expired or deleted between tryStart and get. Try to start again.
                if (!idempotencyService.tryStart(idempotencyKey, Duration.ofMinutes(5), payloadHash)) {
                    throw new IdempotencyConflictException("Concurrent request processing conflict.");
                }
            } else if ("PROCESSING".equals(state.getStatus())) {
                log.warn("Conflict: request with key {} is already being processed", idempotencyKey);
                throw new IdempotencyConflictException("A request with the same Idempotency-Key is currently being processed.");
            } else if ("COMPLETED".equals(state.getStatus())) {
                // Verify payload hash matches — reject reuse with different body
                if (state.getPayloadHash() != null && !state.getPayloadHash().equals(payloadHash)) {
                    log.warn("Idempotency-Key {} reused with different payload. storedHash={}, incomingHash={}",
                            idempotencyKey, state.getPayloadHash(), payloadHash);
                    throw new IdempotencyPayloadMismatchException(
                            "Idempotency-Key has already been used with a different request payload.");
                }
                log.info("Cache hit: returning completed response for key: {}", idempotencyKey);
                return deserializeResponse(joinPoint, state);
            }
        }

        // We successfully set status to PROCESSING. Execute the actual controller logic.
        try {
            Object result = joinPoint.proceed();
            
            // Serialize and cache the successful response
            String responseBodyJson = "";
            int statusCode = 200;

            if (result instanceof ResponseEntity<?> responseEntity) {
                statusCode = responseEntity.getStatusCode().value();
                if (responseEntity.getBody() != null) {
                    responseBodyJson = objectMapper.writeValueAsString(responseEntity.getBody());
                }
            } else if (result != null) {
                responseBodyJson = objectMapper.writeValueAsString(result);
            }

            idempotencyService.complete(idempotencyKey, responseBodyJson, statusCode, payloadHash);
            return result;
        } catch (Throwable e) {
            // CRITICAL: Database transaction or code failed. We MUST clear Redis idempotency key to allow retry!
            log.error("Idempotency target execution failed for key {}. Clearing key from cache.", idempotencyKey, e);
            idempotencyService.fail(idempotencyKey);
            throw e;
        }
    }

    private Object deserializeResponse(ProceedingJoinPoint joinPoint, IdempotencyState state) throws Exception {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Class<?> returnType = signature.getReturnType();

        if (returnType.isAssignableFrom(ResponseEntity.class)) {
            Type genericReturnType = signature.getMethod().getGenericReturnType();
            if (genericReturnType instanceof ParameterizedType parameterizedType) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                if (actualTypeArguments.length > 0) {
                    Class<?> bodyType;
                    if (actualTypeArguments[0] instanceof Class<?>) {
                        bodyType = (Class<?>) actualTypeArguments[0];
                    } else {
                        bodyType = Object.class;
                    }
                    Object body = objectMapper.readValue(state.getResponseBody(), bodyType);
                    return ResponseEntity.status(state.getStatusCode()).body(body);
                }
            }
            Object body = objectMapper.readValue(state.getResponseBody(), Object.class);
            return ResponseEntity.status(state.getStatusCode()).body(body);
        } else {
            return objectMapper.readValue(state.getResponseBody(), returnType);
        }
    }

    /**
     * Computes a SHA-256 hash of the serialized request body DTO arguments.
     * This binds the idempotency key to a specific payload, preventing
     * silent re-use of the same key with a different request body.
     */
    private String computePayloadHash(ProceedingJoinPoint joinPoint) {
        try {
            StringBuilder payloadBuilder = new StringBuilder();
            for (Object arg : joinPoint.getArgs()) {
                if (arg != null && arg.getClass().isRecord()) {
                    payloadBuilder.append(objectMapper.writeValueAsString(arg));
                }
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payloadBuilder.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed by the JVM spec — this cannot happen
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        } catch (Exception e) {
            log.warn("Failed to compute payload hash, falling back to empty hash", e);
            return "";
        }
    }
}
