package com.vmturbo.repository.api;

import javax.annotation.Nonnull;

/**
 * Listener for repository events.
 */
public interface RepositoryListener {
    /**
     * Notifies, that new projected topology is available for operations in the repository.
     *
     * @param projectedTopologyId projected topology id
     * @param topologyContextId context id of the available topology
     */
    default void onProjectedTopologyAvailable(long projectedTopologyId, long topologyContextId) { }

    /**
     * Notifies, that projected topology is failed to be stored in the repository.
     *
     * @param projectedTopologyId projected topology id
     * @param topologyContextId context id of the available topology
     * @param failureDescription description wording of the failure cause
     */
    default void onProjectedTopologyFailure(long projectedTopologyId, long topologyContextId,
            @Nonnull String failureDescription) { }

    /**
     * Notifies, that a new source topology (raw topology sent for analysis)
     * is available for operations in the repository.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     */
    default void onSourceTopologyAvailable(long topologyId, long topologyContextId) { }

    /**
     * Notifies, that a new source topology (raw topology sent for analysis)
     * is failed to be stored in the repository.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     * @param failureDescription description wording of the failure cause
     */
    default void onSourceTopologyFailure(long topologyId, long topologyContextId,
            @Nonnull String failureDescription) { }
}
