package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * An entity has no set of commodities bought by one of its listed providers.
 * If this message is seen during supply chain validation, it probably indicates an error in the
 * construction of the topology graph.
 */
public class MandatoryCommodityBagNotFoundFailure extends EntitySpecificSupplyChainFailure {
    final private TopologyEntity provider;

    /**
     * An entity has no set of commodities bought by one of its listed providers.
     *
     * @param entity the entity.
     * @param provider the specific provider from which the entity does not buy.
     */
    public MandatoryCommodityBagNotFoundFailure(
            @Nonnull TopologyEntity entity,
            @Nonnull TopologyEntity provider) {
        super(
            entity,
            "Entity " + provider.getDisplayName() +
                " is listed as a provider, but no list of commodities bought from it is found.");
        this.provider = provider;
    }

    @Nonnull
    public TopologyEntity getProvider() {
        return provider;
    }
}
