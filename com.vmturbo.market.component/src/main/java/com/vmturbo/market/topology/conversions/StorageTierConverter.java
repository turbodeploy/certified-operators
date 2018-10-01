package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.map.UnmodifiableMap;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.RiskBased;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class StorageTierConverter implements TierConverter {
    TopologyInfo topologyInfo;
    CommodityConverter commodityConverter;
    CostDTOCreator costDTOCreator;

    StorageTierConverter(TopologyInfo topologyInfo, CommodityConverter commodityConverter) {
        this.topologyInfo = topologyInfo;
        this.commodityConverter = commodityConverter;
    }

    StorageTierConverter(TopologyInfo topologyInfo, CommodityConverter commodityConverter,
                         CostLibrary costLibrary) {
        this.topologyInfo = topologyInfo;
        this.commodityConverter = commodityConverter;
        this.costDTOCreator = new CostDTOCreator(commodityConverter, costLibrary);
    }
    /**
     * Create TraderTOs for the storageTier passed in. One traderTO is created for every
     * storageTier, region combination.
     *
     * @param storageTier the storageTier for which the traderTOs need to be created
     * @return Map of {@link TraderTO.Builder} created to {@link MarketTier}
     */
    @Override
    @Nonnull
    public Map<TraderTO.Builder, MarketTier> createMarketTierTraderTOs(
            @Nonnull TopologyEntityDTO storageTier,
            @Nonnull Map<Long, TopologyEntityDTO> topology,
            @Nonnull Set<TopologyEntityDTO> businessAccounts) {
        Map<TraderTO.Builder, MarketTier> traderTOs = new HashMap<>();
        List<TopologyEntityDTO> connectedRegions = TopologyDTOUtil.getConnectedEntitiesOfType(
                storageTier, EntityType.REGION_VALUE, topology);
        for(TopologyEntityDTO region : connectedRegions) {
            MarketTier marketTier = new OnDemandMarketTier(storageTier, region);

            TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils.
                    createCommonTraderSettingsTOBuilder(storageTier, topology,
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
                                    .setCloudCost(costDTOCreator.createStorageTierCostDTO(
                                            storageTier, region, businessAccounts)).build()))
                    .setQuoteFactor(1)
                    .build();

            TraderTO.Builder traderTOBuilder = EconomyDTOs.TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(IdentityGenerator.next())
                    .setType(EntityType.STORAGE_TIER_VALUE)
                    .setState(TopologyConversionUtils.traderState(storageTier))
                    .setSettings(settings)
                    .setTemplateForHeadroom(false)
                    .setDebugInfoNeverUseInCode(
                            TopologyConversionUtils.marketTierEntityDebugInfo(storageTier, region))
                    .addAllCommoditiesSold(commoditiesSoldList(storageTier));
            traderTOs.put(traderTOBuilder, marketTier);
        }
        return traderTOs;
    }

    /**
     * Create the commodities to be sold by compute market tier
     *
     * @param storageTier the storageTier based on which commodities sold are created.
     * @return The commodities to be sold by the marketTier traderTO
     */
    @Nonnull
    private Collection<CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO storageTier) {
        Collection<CommoditySoldTO> commoditiesSold = commodityConverter.commoditiesSoldList(storageTier);
        return commoditiesSold;
    }
}