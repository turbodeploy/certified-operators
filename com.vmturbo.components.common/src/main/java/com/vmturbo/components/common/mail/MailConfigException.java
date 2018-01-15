package com.vmturbo.components.common.mail;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Thrown to indicate invalid or missing SMTP configurations exist.
 */
public class MailConfigException extends Exception {
    public MailConfigException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    public MailConfigException(@Nonnull final String message,
                         @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}
