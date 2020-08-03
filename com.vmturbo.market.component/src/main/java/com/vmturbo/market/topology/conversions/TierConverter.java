package com.vmturbo.market.topology.conversions;

import java.util.Map;
import java.util.Set;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

public interface TierConverter {

    /**
     * Create TraderTOs for the TopologyEntityDTO tier.
     *
     * @param tier The tier for which the traderTOs need to be created
     * @param topology The topology
     * @param businessAccounts The set of business accounts.
     * @param uniqueAccountPricingData The set of unique pricing data (price table + discount) we have in the topology
     *
     * @return Map of {@link TraderTO.Builder} created to {@link MarketTier}
     */
    Map<TraderTO.Builder, MarketTier> createMarketTierTraderTOs(
            TopologyEntityDTO tier, Map<Long, TopologyEntityDTO> topology,
            Set<TopologyEntityDTO> businessAccounts, Set<AccountPricingData> uniqueAccountPricingData);
}
