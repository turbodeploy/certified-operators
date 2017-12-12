package com.vmturbo.sql.utils;

import javax.annotation.Nonnull;

/**
 * Checked exception to be thrown from various DB operations.
 */
public class DbException extends Exception {
    /**
     * Consctructs exception with message and root cause.
     *
     * @param message exception message
     * @param cause exception cause
     */
    public DbException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    /**
     * Consctructs exception with message.
     *
     * @param message exception message
     */
    public DbException(@Nonnull String message) {
        super(message);
    }
}
