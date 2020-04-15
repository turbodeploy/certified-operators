package com.vmturbo.market.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

/**
 * This interface represents MarketTiers (TemplateProvider in classic terminology).
 * MarketTiers can be of two types - on demand or discounted. A discounted MarketTier(RIDiscountedMarketTier)
 * is represented as a SingleRegionMarketTier (created per region). These tiers are converted to traderTOs
 * which will then act as suppliers for shopping lists for Analysis library.
 */
public interface MarketTier {

    /**
     * Return if {@link MarketTier} has RI discount.
     *
     * @return true if {@link MarketTier} has RI discount. Otherwise, false
     */
    boolean hasRIDiscount();

    /**
     * Return if {@link MarketTier} is single region.
     *
     * @return true if {@link MarketTier} is single region.. Otherwise, false
     */
    boolean isSingleRegion();

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
    default List<MarketTier> getConnectedMarketTiersOfType(
            int connectedMarketTierType, @Nonnull Map<Long, TopologyEntityDTO> topology) {
        List<MarketTier> connectedMarketTiers = new ArrayList<>();
        List<TopologyEntityDTO> connectedEntities = TopologyDTOUtil.getConnectedEntitiesOfType(
                getTier(), connectedMarketTierType, topology);
        for (TopologyEntityDTO connectedEntity : connectedEntities) {
        MarketTier marketTier = new OnDemandMarketTier(connectedEntity);
        connectedMarketTiers.add(marketTier);
        }
        return connectedMarketTiers;
    }


    /**
     * Gets the Tier associated with the market tier object.
     *
     * @return The topology entity DTO tier object.
     */
    TopologyEntityDTO getTier();

    /**
     * Gets the display name.
     *
     * @return the display name.
     */
    String getDisplayName();
}

