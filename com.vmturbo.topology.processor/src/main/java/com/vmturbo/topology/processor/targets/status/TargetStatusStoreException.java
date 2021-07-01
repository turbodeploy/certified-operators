package com.vmturbo.topology.processor.targets.status;

import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Exception thrown when face issues doing operations with target status store.
 */
public class TargetStatusStoreException extends TargetStoreException {
    private static final long serialVersionUID = -6666L;

    /**
     * Constructor.
     *
     * @param errorMsg the error message
     */
    public TargetStatusStoreException(String errorMsg) {
        super(errorMsg);
    }

    /**
     * Constructor.
     *
     * @param cause the cause of the error
     */
    public TargetStatusStoreException(Exception cause) {
        super(cause);
    }
}
