package com.vmturbo.cloud.common.scope;

import java.io.IOException;

/**
 * An exception for an operation related to cloud scope identities.
 */
public class IdentityOperationException extends IOException {

    /**
     * Constructs from an underlying {@code cause}.
     * @param cause The underlying exception.
     */
    public IdentityOperationException(Throwable cause) {
        super(cause);
    }
}
