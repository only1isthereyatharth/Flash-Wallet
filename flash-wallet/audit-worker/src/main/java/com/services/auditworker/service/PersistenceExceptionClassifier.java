package com.services.auditworker.service;

import org.hibernate.exception.ConstraintViolationException;

final class PersistenceExceptionClassifier {

    private static final String UNIQUE_VIOLATION_SQL_STATE = "23505";

    private PersistenceExceptionClassifier() {
    }

    static boolean isDuplicateConstraint(Throwable throwable, String constraintName) {
        Throwable current = throwable;

        while (current != null) {
            if (current instanceof ConstraintViolationException constraintViolationException) {
                String currentConstraintName = constraintViolationException.getConstraintName();
                if (constraintName.equalsIgnoreCase(currentConstraintName)) {
                    return true;
                }

                if (constraintViolationException.getSQLException() != null
                        && UNIQUE_VIOLATION_SQL_STATE.equals(constraintViolationException.getSQLException().getSQLState())) {
                    return true;
                }
            }

            String message = current.getMessage();
            if (message != null && message.contains(constraintName)) {
                return true;
            }

            current = current.getCause();
        }

        return false;
    }
}
