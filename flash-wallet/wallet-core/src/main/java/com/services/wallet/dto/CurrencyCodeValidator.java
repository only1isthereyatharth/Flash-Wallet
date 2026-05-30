package com.services.wallet.dto;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Currency;

public class CurrencyCodeValidator implements ConstraintValidator<CurrencyCode, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (value == null) {
            // Let @NotNull handle null checks
            return true;
        }
        try {
            Currency.getInstance(value.toUpperCase(java.util.Locale.ROOT));
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
