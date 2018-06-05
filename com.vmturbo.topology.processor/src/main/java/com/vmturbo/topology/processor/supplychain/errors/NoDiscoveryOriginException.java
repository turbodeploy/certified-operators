package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * We cannot determine the origin of an entity (discovering targets).
 */
public class NoDiscoveryOriginException extends EntitySpecificSupplyChainException {
    /**
     * No discovery origin found.
     *
     * @param entity entity for which there is no discovery origin.
     */
    public NoDiscoveryOriginException(@Nonnull TopologyEntity entity) {
        super(entity, "No discovery origin found.");
    }
}
