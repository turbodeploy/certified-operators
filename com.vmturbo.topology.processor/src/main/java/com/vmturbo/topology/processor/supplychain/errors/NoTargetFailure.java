package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * A target cannot be found.
 */
public class NoTargetFailure extends SupplyChainValidationFailure {
    /**
     * A target with a specific id cannot be found.
     *
     * @param entity  entity during the verification of which the problem was discovered.
     * @param targetId id of the missing target.
     */
    public NoTargetFailure(@Nonnull TopologyEntity entity, long targetId) {
        super(null, Long.toString(targetId), entity.getDisplayName(), "Target not found");
    }
}
