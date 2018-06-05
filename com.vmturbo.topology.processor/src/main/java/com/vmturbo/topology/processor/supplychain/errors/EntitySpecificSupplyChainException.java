package com.vmturbo.topology.processor.supplychain.errors;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * Generic exception class for supply chain errors that have to do with a specific entity.
 */
public class EntitySpecificSupplyChainException extends SupplyChainValidationException {
    private final TopologyEntity entity;

    /**
     * Generic error related to a specific entity.
     *
     * @param entity the entity.
     * @param message description of the error.
     */
    public EntitySpecificSupplyChainException(@Nonnull TopologyEntity entity, @Nonnull String message) {
        super(null, null, entity.getDisplayName(), message);
        this.entity = entity;
    }

    @Nonnull
    public TopologyEntity getEntity() {
        return entity;
    }
}
