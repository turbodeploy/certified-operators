package com.vmturbo.topology.processor.group;

import javax.annotation.Nonnull;

/**
 * An exception that may be thrown when resolving the definition for a group.
 */
public class GroupResolutionException extends RuntimeException {
    public GroupResolutionException(@Nonnull final RuntimeException e) {
        super(e);
    }

    public GroupResolutionException(@Nonnull final String message) {
        super(message);
    }
}
