package com.vmturbo.topology.processor.targets;

/**
 * Exception to be thrown when target is expected to be in a target store, but it is not.
 */
public class TargetNotFoundException extends TargetStoreException {

    private static final long serialVersionUID = 1L;

    public TargetNotFoundException(long targetId) {
        super("Target with id " + targetId + " does not exist in the store.");
    }
}
