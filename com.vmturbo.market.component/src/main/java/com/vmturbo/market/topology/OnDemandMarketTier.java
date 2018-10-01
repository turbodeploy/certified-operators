package com.vmturbo.market.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.map.UnmodifiableMap;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


/**
 * The combination of a tier (compute tier / storage tier / database tier) with region is
 * represented as OnDemandMarketTier.
 * Each TraderTO created in one of the TierConverters created maps to a OnDemandMarketTier.
 */
public class OnDemandMarketTier implements MarketTier {
    private final TopologyEntityDTO tier;
    private final TopologyEntityDTO region;

    public OnDemandMarketTier(@Nonnull TopologyEntityDTO tier,
                             @Nonnull TopologyEntityDTO region) {
        int tierType = tier.getEntityType();
        if (!TopologyConversionConstants.TIER_ENTITY_TYPES.contains(tierType)
                || region.getEntityType() != EntityType.REGION_VALUE) {
            throw new IllegalArgumentException("Invalid arguments to construct " +
                    "OnDemandMarketTier " + tier.getDisplayName() + ", " + region.getDisplayName());
        }
        this.tier = tier;
        this.region = region;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tier, region);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof OnDemandMarketTier))
            return false;

        OnDemandMarketTier otherOnDemandMarketTier = (OnDemandMarketTier) other;

        return otherOnDemandMarketTier.getTier() == this.getTier()
                && otherOnDemandMarketTier.getRegion() == this.getRegion();
    }

    @Override
    @Nonnull
    public TopologyEntityDTO getTier() {
        return tier;
    }

    @Override
    @Nonnull
    public TopologyEntityDTO getRegion() {
        return region;
    }

    /**
     * On demand market tier has no RI discount.
     * @return OnDemandMarketTier has no discount
     */
    @Override
    public boolean hasRIDiscount() {
        return false;
    }

    /**
     * Gets the connected market tiers of specified type.
     * For ex. lets say 'this' represents [Region1 x ComputeTier1] and ComputeTier1 is connected to
     * 2 storage types io1 and gp2. Then this method called with EntityType.STORAGE_TIER_VALUE
     * will return 2 MarketTiers - [Region1 x io1] and [Region1 x gp2].
     *
     * @param connectedMarketTierType The EntityType of connected TopologyEntityDTO
     * @param topology the topology
     * @return List of connected MarketTiers
     */
    @Override
    @Nonnull
    public List<MarketTier> getConnectedMarketTiersOfType(
            int connectedMarketTierType,
            @Nonnull Map<Long, TopologyEntityDTO> topology) {
        List<MarketTier> connectedMarketTiers = new ArrayList<>();
        List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(
                getTier(), connectedMarketTierType, topology);
        for(TopologyEntityDTO connectedEntity : connectedEntities) {
            MarketTier marketTier = new OnDemandMarketTier(connectedEntity, getRegion());
            connectedMarketTiers.add(marketTier);
        }
        return connectedMarketTiers;
    }
}