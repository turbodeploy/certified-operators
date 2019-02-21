package com.vmturbo.topology.processor.targets;

import javax.annotation.Nonnull;

/**
 * Exception thrown during target registration if there is already an existing target in target store.
 */
public class DuplicateTargetException extends TargetStoreException {

    public DuplicateTargetException(@Nonnull final String targetAddr) {
        super(String.format("Target %s already exists.", targetAddr));
    }
}
