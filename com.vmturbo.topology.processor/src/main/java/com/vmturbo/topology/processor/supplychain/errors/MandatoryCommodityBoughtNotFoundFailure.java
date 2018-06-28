package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Missing mandatory commodity bought.
 */
public class MandatoryCommodityBoughtNotFoundFailure extends EntitySpecificSupplyChainFailure {
    final private TemplateCommodity commodity;
    final private TopologyEntity provider;

    /**
     * A commodity that the entity must buy does not exist.
     *
     * @param entity the entity.
     * @param commodity missing commodity.
     * @param provider the specific provider from which the entity does not buy.
     */
    public MandatoryCommodityBoughtNotFoundFailure(
            @Nonnull TopologyEntity entity,
            @Nonnull TemplateCommodity commodity,
            @Nonnull TopologyEntity provider) {
        super(
            entity,
            "Mandatory commodity bought " + commodity.getCommodityType().name() + " (from provider " +
                provider.getDisplayName() + " of type " + provider.getEntityType() + ") not found.");
        this.commodity = commodity;
        this.provider = provider;
    }

    @Nonnull
    public TemplateCommodity getCommodity() {
        return commodity;
    }

    @Nonnull
    public TopologyEntity getProvider() {
        return provider;
    }
}
