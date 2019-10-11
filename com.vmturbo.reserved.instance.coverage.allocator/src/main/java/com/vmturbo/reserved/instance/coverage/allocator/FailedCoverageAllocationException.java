package com.vmturbo.reserved.instance.coverage.allocator;

/**
 * A wrapper class around exceptions thrown by a concurrent coverage allocation step. This wrapper
 * is used to facilitate lambda operations on {@link java.util.concurrent.Future} instances.
 */
public class FailedCoverageAllocationException extends RuntimeException {

    /**
     * Wrap an exception thrown during coverage allocation
     * @param cause The wrapped exception
     */
    public FailedCoverageAllocationException(final Throwable cause) {
        super(cause);
    }
}
