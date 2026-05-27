package com.services.wallet.idempotency;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.util.HtmlUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;

@Aspect
@Component
@Order(1) // Run after validation phase has successfully completed
@Slf4j
public class SanitizationAspect {
    
    private static final String REST_CONTROLLER_POINTCUT = 
        "@within(org.springframework.web.bind.annotation.RestController)";

    @Around(REST_CONTROLLER_POINTCUT)
    public Object sanitizeRestControllerInputs(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs();
        boolean modified = false;

        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg != null && arg.getClass().isRecord()) {
                Object sanitized = sanitizeRecord(arg);
                if (sanitized != arg) {
                    args[i] = sanitized;
                    modified = true;
                    log.debug("Sanitized DTO of type {} in controller request mapping", arg.getClass().getSimpleName());
                }
            }
        }

        if (modified) {
            return joinPoint.proceed(args);
        }
        return joinPoint.proceed();
    }

    private Object sanitizeRecord(Object recordObj) {
        Class<?> clazz = recordObj.getClass();
        RecordComponent[] components = clazz.getRecordComponents();
        Object[] values = new Object[components.length];
        boolean changed = false;

        try {
            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                Object val = component.getAccessor().invoke(recordObj);

                if (val instanceof String) {
                    String strVal = (String) val;
                    String sanitized = sanitizeString(strVal);
                    if (!sanitized.equals(strVal)) {
                        values[i] = sanitized;
                        changed = true;
                    } else {
                        values[i] = strVal;
                    }
                } else if (val != null && val.getClass().isRecord()) {
                    Object sanitized = sanitizeRecord(val);
                    if (sanitized != val) {
                        values[i] = sanitized;
                        changed = true;
                    } else {
                        values[i] = val;
                    }
                } else {
                    values[i] = val;
                }
            }

            if (changed) {
                Class<?>[] paramTypes = Arrays.stream(components)
                        .map(RecordComponent::getType)
                        .toArray(Class[]::new);
                Constructor<?> constructor = clazz.getDeclaredConstructor(paramTypes);
                constructor.setAccessible(true);
                return constructor.newInstance(values);
            }
        } catch (Exception e) {
            log.error("Failed to sanitize record parameter of type: {}", clazz.getName(), e);
        }
        return recordObj;
    }

    private String sanitizeString(String input) {
        if (input == null) {
            return null;
        }
        // Trim leading/trailing whitespaces and escape HTML special characters to prevent XSS
        return HtmlUtils.htmlEscape(input.trim());
    }
}
