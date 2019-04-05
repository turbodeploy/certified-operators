package com.vmturbo.components.common.mail;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Thrown to indicate empty smtpServer or fromAddress exists.
 */
public class MailEmptyConfigException extends MailConfigException {
    public MailEmptyConfigException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    public MailEmptyConfigException(@Nonnull final String message,
                               @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}