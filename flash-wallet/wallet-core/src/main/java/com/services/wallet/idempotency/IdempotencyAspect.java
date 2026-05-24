package com.services.wallet.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.services.wallet.exception.IdempotencyConflictException;
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
import java.time.Duration;

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

        log.info("Processing idempotent request with key: {}", idempotencyKey);

        // Try to start processing. Guard TTL set to 5 minutes to release keys if service crashes mid-flight.
        boolean started = idempotencyService.tryStart(idempotencyKey, Duration.ofMinutes(5));

        if (!started) {
            // Key already exists. Fetch the state from Redis.
            IdempotencyState state = idempotencyService.get(idempotencyKey);
            if (state == null) {
                // Edge case: state expired or deleted between tryStart and get. Try to start again.
                if (!idempotencyService.tryStart(idempotencyKey, Duration.ofMinutes(5))) {
                    throw new IdempotencyConflictException("Concurrent request processing conflict.");
                }
            } else if ("PROCESSING".equals(state.getStatus())) {
                log.warn("Conflict: request with key {} is already being processed", idempotencyKey);
                throw new IdempotencyConflictException("A request with the same Idempotency-Key is currently being processed.");
            } else if ("COMPLETED".equals(state.getStatus())) {
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

            idempotencyService.complete(idempotencyKey, responseBodyJson, statusCode);
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
}
