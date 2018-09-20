package com.vmturbo.topology.processor.targets;

/**
 * Top level target exception class. The other kind of target exceptions should extend from it.
 */
public class TargetStoreException extends Exception {
    private static final long serialVersionUID = -6666L;

    public TargetStoreException(String errorMsg) {
        super(errorMsg);
    }

    public TargetStoreException(Exception cause) {
        super(cause);
    }
}
