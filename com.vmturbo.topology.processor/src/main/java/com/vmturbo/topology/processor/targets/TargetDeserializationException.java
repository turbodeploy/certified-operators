package com.vmturbo.topology.processor.targets;

/**
 * Exception when deserializing a {@link Target} from a string fails.
 */
public class TargetDeserializationException extends TargetStoreException {
    public TargetDeserializationException(Exception cause) {
        super(cause);
    }
}
