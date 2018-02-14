package com.vmturbo.reports.component;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.mail.EmailException;
import org.apache.commons.validator.EmailValidator;

/**
 * Class to validate email addresses. Current implementation is using apache validator.
 */
public class EmailAddressValidator {

    private static final EmailValidator EMAIL_VALIDATOR = EmailValidator.getInstance();

    /**
     * Validates email addresses using apache commons validator.
     *
     * @param emailAddresses to validate
     * @throws EmailException if at least one of provided address isn't valid
     */
    public static void validateAddresses(@Nonnull List<String> emailAddresses) throws EmailException {
        for (String email : emailAddresses) {
            validateAddress(email);
        }
    }

    /**
     * Validates email address using apache commons validator.
     *
     * @param email to validate
     * @throws EmailException if address isn't valid
     */
    public static void validateAddress(@Nonnull String email) throws EmailException {
        if (!EMAIL_VALIDATOR.isValid(email)) {
            throw new EmailException("Not valid email provided: " + email);
        }
    }
}
