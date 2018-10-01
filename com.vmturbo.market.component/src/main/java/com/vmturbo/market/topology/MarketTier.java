package com.vmturbo.market.topology;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.map.UnmodifiableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * This interface represents MarketTiers (TemplateProvider in classic terminology).
 * MarketTiers can be of two types - on demand or discounted.
 * These tiers are converted to traderTOs which will then act as suppliers for shopping lists
 * for Analysis library.
 */
public interface MarketTier {

    boolean hasRIDiscount();

    List<MarketTier> getConnectedMarketTiersOfType(
            int connectedMarketTierType, @Nonnull Map<Long, TopologyEntityDTO> topology);

    TopologyEntityDTO getTier();

    TopologyEntityDTO getRegion();
}
