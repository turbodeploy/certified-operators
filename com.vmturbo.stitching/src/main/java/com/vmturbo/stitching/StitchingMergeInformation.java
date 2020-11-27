package com.vmturbo.stitching;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.stitching.utilities.DTOFieldAndPropertyHandler;

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
    private final StitchingErrors error;
    private final String vendorId;
    private final EntityOrigin origin;

    /**
     * Create {@link StitchingMergeInformation} associated with a particular entity.
     *
     * @param entity The entity whose {@link StitchingMergeInformation} should be tracked.
     */
    public StitchingMergeInformation(@Nonnull final StitchingEntity entity) {
        this(entity.getOid(), entity.getTargetId(), entity.getStitchingErrors(),
                DTOFieldAndPropertyHandler.getVendorId(entity.getEntityBuilder()),
                entity.getEntityBuilder().getOrigin());
    }

    /**
     * Create a {@Link StitchingMergeInformation} describing a particular entity discovered
     * by a particular target.
     *
     * @param oid The oid of the entity.
     * @param targetId The id of the target that discvoered the entity with the given oid.
     * @param errorCode collection of errors applicable to an entity
     */
    public StitchingMergeInformation(final long oid, final long targetId,
                                     final StitchingErrors errorCode) {
        this(oid, targetId, errorCode, null, EntityOrigin.DISCOVERED);
    }

    /**
     * Create a {@Link StitchingMergeInformation} describing a particular entity discovered
     * by a particular target.
     *
     * @param oid The oid of the entity.
     * @param targetId The id of the target that discvoered the entity with the given oid.
     * @param errorCode collection of errors applicable to an entity
     * @param vendorId external identifier on a target
     * @param origin the origin type.
     */
    public StitchingMergeInformation(final long oid, final long targetId,
            @Nonnull final StitchingErrors errorCode, @Nullable String vendorId,
            @Nonnull EntityOrigin origin) {
        this.oid = oid;
        this.targetId = targetId;
        this.error = errorCode;
        this.vendorId = vendorId;
        this.origin = origin;
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

    @Nonnull
    public StitchingErrors getError() {
        return error;
    }

    @Nullable
    public String getVendorId() {
        return vendorId;
    }

    @Nonnull
    public EntityOrigin getOrigin() {
        return origin;
    }

    @Override
    public String toString() {
        return formatOidAndTarget(oid, targetId);
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        if (other == null || !(other instanceof StitchingMergeInformation)) {
            return false;
        }

        final StitchingMergeInformation smi = (StitchingMergeInformation)other;
        // Deliberately exclude the errors.
        return oid == smi.oid && targetId == smi.targetId;
    }

    @Override
    public int hashCode() {
        // Deliberately exclude the errors.
        return Objects.hashCode(oid, targetId);
    }

    @Nonnull
    public static String formatOidAndTarget(final long oid, final long targetId) {
        return "(oid-" + oid + " " + "tgt-" + targetId +")";
    }

}
