package com.vmturbo.market.topology.conversions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.map.UnmodifiableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

public class DatabaseTierConverter implements TierConverter{
    TopologyInfo topologyInfo;

    DatabaseTierConverter(TopologyInfo topologyInfo) {
        this.topologyInfo = topologyInfo;
    }
    /**
     * Create TraderTOs for the dbTier passed in. One traderTO is created for every
     * dbTier, region combination.
     *
     * @param dbTier the dbTier for which the traderTOs need to be created
     * @return Map of {@link TraderTO.Builder} created to {@link MarketTier}
     */
    @Override
    @Nonnull
    public Map<TraderTO.Builder, MarketTier> createMarketTierTraderTOs(
            @Nonnull TopologyEntityDTO dbTier,
            @Nonnull Map<Long, TopologyEntityDTO> topology,
            @Nonnull Set<TopologyEntityDTO> businessAccounts) {
        Map<TraderTO.Builder, MarketTier> traderTOs = new HashMap<>();
        return traderTOs;
    }
}