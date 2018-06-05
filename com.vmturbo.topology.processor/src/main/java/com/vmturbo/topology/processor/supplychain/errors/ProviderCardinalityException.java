package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * Providers of a specific type do not respect the cardinality requirements.
 */
public class ProviderCardinalityException extends EntitySpecificSupplyChainException {
    /**
     * Provider cardinality error.
     *
     * @param entity entity with erroneous number of providers.
     * @param providerType type of providers.
     * @param min minimum acceptable number of providers of this type.
     * @param max maximum acceptable number of providers of this type.
     * @param actual actual number of providers of this type.
     */
    public ProviderCardinalityException(
        @Nonnull TopologyEntity entity, int providerType, int min, int max, int actual
    ) {
        super(
             entity,
             "Provider cardinality constraints violated. An entity of type " + entity.getEntityType() +
                 " must have at least " + min + " and at most " + max + " provider(s) of type " +
                 providerType + ".  This entity has " + actual + ".");
    }
}
