package com.vmturbo.topology.processor.targets.status;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;

/**
 * Interface for CRUD operations with target status details.
 */
public interface TargetStatusStore {

    /**
     * Persist information describes status of the target.
     *
     * @param targetStatus target status info
     * @throws TargetStatusStoreException if failed to persist status for the target
     */
    void setTargetStatus(TargetStatus targetStatus) throws TargetStatusStoreException;

    /**
     * Delete status info fot the target.
     *
     * @param targetId the target id
     * @throws TargetStatusStoreException if failed to delete status for the target
     */
    void deleteTargetStatus(long targetId) throws TargetStatusStoreException;

    /**
     * Get statuses for the requested targets. If input target ids is null then return all existed
     * targets statuses.
     *
     * @param targetIds target ids
     * @return the map of targets statuses
     * @throws TargetStatusStoreException if failed to get statuses for the targets
     */
    @Nonnull
    Map<Long, TargetStatus> getTargetsStatuses(@Nullable Collection<Long> targetIds)
            throws TargetStatusStoreException;
}
