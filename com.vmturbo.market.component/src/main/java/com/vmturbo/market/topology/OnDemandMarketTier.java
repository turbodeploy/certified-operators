package com.vmturbo.market.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


/**
 * The combination of a tier (compute tier / storage tier / database tier) with region is
 * represented as OnDemandMarketTier.
 * Each TraderTO created in one of the TierConverters created maps to a OnDemandMarketTier.
 */
public class OnDemandMarketTier implements MarketTier {
    private final TopologyEntityDTO tier;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasRIDiscount() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSingleRegion() {
        return false;
    }

    /**
     * Constructor for the on Demand market tier.
     *
     * @param tier tier we want to create a market tier object for.
     */
    public OnDemandMarketTier(@Nonnull TopologyEntityDTO tier) {
        int tierType = tier.getEntityType();
        if (!TopologyDTOUtil.isTierEntityType(tierType)) {
            throw new IllegalArgumentException("Invalid arguments to construct " +
                    "OnDemandMarketTier " + tier.getDisplayName());
        }
        this.tier = tier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tier);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof OnDemandMarketTier))
            return false;

        OnDemandMarketTier otherOnDemandMarketTier = (OnDemandMarketTier) other;

        return otherOnDemandMarketTier.getTier() == this.getTier();
    }

    @Override
    @Nonnull
    public TopologyEntityDTO getTier() {
        return tier;
    }

    @Override
    @Nonnull
    public String getDisplayName() {
        return EntityType.forNumber(tier.getEntityType()) + "|"
                + tier.getDisplayName();
    }
}
