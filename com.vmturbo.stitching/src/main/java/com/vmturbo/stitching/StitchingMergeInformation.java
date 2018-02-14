package com.vmturbo.stitching;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

/**
 * Information about an entity that was merged onto an entity.
 *
 * In the parlance of the terminology used by
 * {@link com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails}, if we have two entities A and B,
 * and we merge information from entity A onto entity B, then entity B will be augmented with a
 * {@link StitchingMergeInformation} associated with entity A to track the merger.
 */
public class StitchingMergeInformation {
    private final long oid;
    private final long targetId;

    /**
     * Create {@link StitchingMergeInformation} associated with a particular entity.
     *
     * @param entity The entity whose {@link StitchingMergeInformation} should be tracked.
     */
    public StitchingMergeInformation(@Nonnull final StitchingEntity entity) {
        this(entity.getOid(), entity.getTargetId());
    }

    /**
     * Create a {@Link StitchingMergeInformation} describing a particular entity discovered
     * by a particular target.
     *
     * @param oid The oid of the entity.
     * @param targetId The id of the target that discvoered the entity with the given oid.
     */
    public StitchingMergeInformation(final long oid, final long targetId) {
        this.oid = oid;
        this.targetId = targetId;
    }

    /**
     * Get the oid for the entity associated with this merge information.
     *
     * @return
     */
    public long getOid() {
        return oid;
    }

    public long getTargetId() {
        return targetId;
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        if (other == null || !(other instanceof StitchingMergeInformation)) {
            return false;
        }

        final StitchingMergeInformation smi = (StitchingMergeInformation)other;
        return oid == smi.oid && targetId == smi.targetId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(oid, targetId);
    }
}
