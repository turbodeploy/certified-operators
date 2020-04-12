package com.vmturbo.topology.processor.group;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;

/**
 * An exception that may be thrown when resolving the definition for a group.
 */
public class GroupResolutionException extends Exception {

    /**
     * Create a new {@link GroupResolutionException}.
     *
     * @param group The group that failed to be resolved.
     * @param cause The cause of the resolution failure.
     */
    public GroupResolutionException(final Grouping group, @Nonnull final Exception cause) {
        super("Failed to resolve group " + group.getId() + "(" + group.getDefinition().getDisplayName() + ")", cause);
    }
}
