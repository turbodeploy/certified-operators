package com.vmturbo.topology.processor.supplychain.errors;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * A target cannot be found.
 */
public class NoTargetException extends SupplyChainValidationException {
    /**
     * A target with a specific id cannot be found.
     *
     * @param entity  entity during the verification of which the problem was discovered.
     * @param targetId id of the missing target.
     */
    public NoTargetException(@Nonnull TopologyEntity entity, long targetId) {
        super(null, Long.toString(targetId), entity.getDisplayName(), "Target not found");
    }
}
