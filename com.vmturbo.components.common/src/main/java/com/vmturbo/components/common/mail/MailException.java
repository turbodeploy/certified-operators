package com.vmturbo.components.common.mail;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Exception to be thrown when error occurred when sending an email.
 */
public class MailException extends Exception {
    public MailException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    public MailException(@Nonnull final String message,
                              @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}
