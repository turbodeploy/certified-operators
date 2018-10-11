package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.RiskBased;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class is used to create TraderTOs for the combination of compute tier and region.
 *
 */
public class ComputeTierConverter implements TierConverter {
    private static final Logger logger = LogManager.getLogger();
    TopologyInfo topologyInfo;
    CommodityConverter commodityConverter;
    CostDTOCreator costDTOCreator;

    ComputeTierConverter(TopologyInfo topologyInfo, CommodityConverter commodityConverter,
                         @Nonnull CostDTOCreator costDTOCreator) {
        this.topologyInfo = topologyInfo;
        this.commodityConverter = commodityConverter;
        this.costDTOCreator = costDTOCreator;
    }

    /**
     * Create TraderTOs for the computeTier passed in. One traderTO is created for every
     * computeTier, region combination.
     *
     * @param computeTier the computeTier for which the traderTOs need to be created
     * @return Map of {@link TraderTO.Builder} created to {@link MarketTier}
     */
    @Override
    @Nonnull
    public Map<TraderTO.Builder, MarketTier> createMarketTierTraderTOs(
            @Nonnull TopologyEntityDTO computeTier,
            @Nonnull Map<Long, TopologyEntityDTO> topology,
            @Nonnull Set<TopologyEntityDTO> businessAccounts) {
        Map<TraderTO.Builder, MarketTier> traderTOs = new HashMap<>();
        List<TopologyEntityDTO> connectedRegions = TopologyDTOUtil.getConnectedEntitiesOfType(
                computeTier, EntityType.REGION_VALUE, topology);
        for(TopologyEntityDTO region : connectedRegions) {
            MarketTier marketTier = new OnDemandMarketTier(computeTier, region);
            String debugInfo = marketTier.getDisplayName();
            logger.debug("Creating trader for {}", debugInfo);
            TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils.
                    createCommonTraderSettingsTOBuilder(computeTier, topology,
                            TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo));
            final EconomyDTOs.TraderSettingsTO settings = settingsBuilder
                    .setClonable(false)
                    .setSuspendable(false)
                    // TODO: For canAcceptNewCustomers - Need to check if price is available.
                    .setCanAcceptNewCustomers(true)
                    // TODO: Check why isEligibleForResizeDown is true for computeTier?
                    .setIsEligibleForResizeDown(false)
                    .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                            .setRiskBased(RiskBased.newBuilder()
                                    .setCloudCost(costDTOCreator.createCostDTO(computeTier, region, businessAccounts)).build()))
                    .setQuoteFactor(1)
                    .build();

            TraderTO.Builder traderTOBuilder = EconomyDTOs.TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(IdentityGenerator.next())
                    .setType(computeTier.getEntityType())
                    .setState(TopologyConversionUtils.traderState(computeTier))
                    .setSettings(settings)
                    .setTemplateForHeadroom(false)
                    .setDebugInfoNeverUseInCode(debugInfo)
                    .addAllCommoditiesSold(commoditiesSoldList(computeTier, region));
            traderTOs.put(traderTOBuilder, marketTier);
        }
        return traderTOs;
    }

    /**
     * Create the commodities to be sold by compute market tier.
     *
     * @param computeTier the computeTier based on which commodities sold are created.
     * @param region the region based on which commodities sold are created.
     * @return The commodities to be sold by the marketTier traderTO
     */
    @Nonnull
    private Collection<CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO computeTier, TopologyEntityDTO region) {
        Collection<CommoditySoldTO> commoditiesSold = commodityConverter.commoditiesSoldList(computeTier);
        commoditiesSold.addAll(commodityConverter.commoditiesSoldList(region));
        return commoditiesSold;
    }
}