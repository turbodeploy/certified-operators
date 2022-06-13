package com.vmturbo.sql.utils.partition;

/**
 * Checked exception for failures that can occur in {@link PartitioningManager} methods.
 */
public class PartitionProcessingException extends Exception {
    /**
     * Create a new instance without message or cause.
     */
    public PartitionProcessingException() {
    }

    /**
     * Create a new instance with message and no cause.
     *
     * @param message message
     */
    public PartitionProcessingException(String message) {
        super(message);
    }

    /**
     * Create a new instance with a mesasge and a cause.
     *
     * @param message message
     * @param cause   cause
     */
    public PartitionProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
