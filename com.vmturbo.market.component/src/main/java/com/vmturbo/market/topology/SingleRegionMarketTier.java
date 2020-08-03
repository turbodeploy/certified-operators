package com.vmturbo.market.topology;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * This interface constructs market tiers per region.
 * For example RIDiscountedMarketTiers.
 */
public interface SingleRegionMarketTier extends MarketTier {

    /**
     * {@inheritDoc}
     */
    @Override
    default boolean isSingleRegion() {
        return true;
    }

    /**
     * Gets the region associated with the market tier.
     *
     * @return the region topology entity dto object.
     */
    TopologyEntityDTO getRegion();
}
