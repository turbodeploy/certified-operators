package com.vmturbo.group.persistent;

import javax.annotation.Nonnull;

/**
 * Exception throws if there is an error interacting with the database.
 */
public class DatabaseException extends Exception {

    DatabaseException(@Nonnull final String message, @Nonnull final Throwable cause) {
        super(message, cause);
    }

}
