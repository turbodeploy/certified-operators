package com.vmturbo.kvstore;

import javax.annotation.Nonnull;

/**
 * Domain specific key/value store operation exception to protect us from external changes. It's a
 * wrapper to external key/value operation exceptions, such as Consul.
 */
public class KeyValueStoreOperationException extends RuntimeException {
    private static final long serialVersionUID = -4277740169040038780L;

    /**
     * Constructor.
     *
     * @param message the detail message.
     * @param cause   the cause.
     */
    public KeyValueStoreOperationException(@Nonnull final String message,
            @Nonnull final Throwable cause) {
        super(message, cause);
    }
}
