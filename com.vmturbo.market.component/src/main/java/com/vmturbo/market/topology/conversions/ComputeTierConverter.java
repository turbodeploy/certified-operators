package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.RiskBased;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.IgnoreConsumption;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
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
    TierExcluder tierExcluder;

    ComputeTierConverter(TopologyInfo topologyInfo, CommodityConverter commodityConverter,
                         @Nonnull CostDTOCreator costDTOCreator,
                         @Nonnull TierExcluder tierExcluder) {
        this.topologyInfo = topologyInfo;
        this.commodityConverter = commodityConverter;
        this.costDTOCreator = costDTOCreator;
        this.tierExcluder = tierExcluder;
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
            @Nonnull Set<TopologyEntityDTO> businessAccounts, @Nonnull Set<AccountPricingData> uniqueAccountPricingData) {
        Map<TraderTO.Builder, MarketTier> traderTOs = new HashMap<>();
        List<TopologyEntityDTO> connectedRegions = TopologyDTOUtil.getConnectedEntitiesOfType(
                computeTier, EntityType.REGION_VALUE, topology);
        MarketTier marketTier = new OnDemandMarketTier(computeTier);
        String debugInfo = marketTier.getDisplayName();
        logger.debug("Creating trader for {}", debugInfo);
        TraderSettingsTO.Builder settingsBuilder = TopologyConversionUtils
                .createCommonTraderSettingsTOBuilder(computeTier, topology);
        final EconomyDTOs.TraderSettingsTO settings = settingsBuilder
                .setClonable(false)
                .setSuspendable(false)
                // TODO: For canAcceptNewCustomers - Need to check if price is available.
                .setCanAcceptNewCustomers(true)
                // TODO: Check why isEligibleForResizeDown is true for computeTier?
                .setIsEligibleForResizeDown(false)
                .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                        .setRiskBased(RiskBased.newBuilder()
                                .setCloudCost(costDTOCreator.createCostDTO(computeTier, connectedRegions,
                                        uniqueAccountPricingData)).build()))
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
                .addAllCommoditiesSold(commoditiesSoldList(computeTier, connectedRegions));
        traderTOs.put(traderTOBuilder, marketTier);
        return traderTOs;
    }

    /**
     * Create the commodities to be sold by compute market tier.
     *
     * @param computeTier the computeTier based on which commodities sold are created.
     * @param regions the regions based on which commodities sold are created.
     * @return The commodities to be sold by the marketTier traderTO
     */
    @Nonnull
    protected Collection<CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO computeTier, List<TopologyEntityDTO> regions) {
        Collection<CommoditySoldTO> commoditiesSold = commoditiesSoldFromTier(computeTier);
        UpdatingFunctionTO emptyUf = UpdatingFunctionTO.newBuilder().build();
        for (TopologyEntityDTO region: regions) {
            commoditiesSold.addAll(commoditiesSoldFromRegion(region));
        }
        if (computeTier.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {
            // TODO: sell VMPM
            // A VM in zone1 can only use zonal RIs that are in zone1. Given a region with zonal and
            // regional RIs and only one matching VM whose capacity is greater than any one zonal RI,
            // only the regional RI is utilized, because zonal RIs are not instance size flexible.
            // 1) Use the VMPM_ACCESS commodity, whose key is the availability zone, for VMs, TPs and CBTPs.

            // sell CouponComm
            // Every computeTier's size in terms of number of coupons is specified through the couponComm capacity
            float capacity = computeTier.getTypeSpecificInfo().getComputeTier().getNumCoupons();
            float used = 0;
            CommodityType commType =
                    CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.COUPON_VALUE)
                            .build();
            UpdatingFunctionTO couponUf = UpdatingFunctionTO.newBuilder().setIgnoreConsumption(
                    IgnoreConsumption.newBuilder().build()).build();
            commoditiesSold.add(commodityConverter.createCommoditySoldTO(commType, capacity, used, couponUf));

            // TODO: sell all TenancyAccess commodities
            // VMs buy a specific tenancy and computeTiers need to sell all tenancies

            // sell TemplateAccess
            // Instance size flexible (ISF) RIs can consider all the templates in a family while applying
            // discount while non ISF can aply discount only on a particular template.
            // 1) A template access commodity with a key set to a template type, TemplateAccess::t2.small,
            // will be bought by a non-ISF (Instance size flexible) CBTP. In order to only match it with
            // vms of a certain template. e.g. t2.small
            // 2) A template access commodity with key set to a template family, e.g. TemplateAccess::t2
            // will be bought by an ISF (Instance size flexible) CBTP, In order to allow matching the CBTP
            // with vms of all template in the family. e.g. t2.small, t.large, t2.nano etc.
            // 3) When matching a VM to a CBTP, we try to find the template that the VM matches to while
            // applying the discount. While doing this, we must keep the region boundaries in mind.
            // Say the VM needs m4.large and there is no M4 large in the region where there is a regional RI,
            // we must make the CBTP aware of this. To make this happen, we make the TP sell a TemplateAccessCommodity
            // with the key as the DC commodity sold by the TP. We make the CBTP buy TemplateAccessCommodity
            // with a DC commodity sold key. Now when looking for the TP while matching to a CBTP,
            // we will find only the TPs present in the region.
            capacity = TopologyConversionConstants.ACCESS_COMMODITY_CAPACITY;
            used = 0;
            commType = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE)
                    .setKey(computeTier.getTypeSpecificInfo().getComputeTier().getFamily())
                    .build();
            commoditiesSold.add(commodityConverter.createCommoditySoldTO(commType, capacity, used,
                    emptyUf));
            commType = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE)
                    .setKey(computeTier.getDisplayName())
                    .build();
            commoditiesSold.add(commodityConverter.createCommoditySoldTO(commType, capacity, used,
                    emptyUf));
            for (TopologyEntityDTO region : regions) {
                commType = CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE)
                        // using regionName as the key for the region specific templateAccessSold
                        .setKey(region.getDisplayName())
                        .build();
                commoditiesSold.add(commodityConverter.createCommoditySoldTO(commType, capacity, used, emptyUf));
            }
        }
        // Add template exclusion commodities sold
        commoditiesSold.addAll(tierExcluder.getTierExclusionCommoditiesToSell(computeTier.getOid()).stream()
            .map(ct -> commodityConverter.createCommoditySoldTO(
                ct, TopologyConversionConstants.ACCESS_COMMODITY_CAPACITY, 0, emptyUf))
            .collect(Collectors.toList()));
        return commoditiesSold;
    }

    /**
     * Create the region commodities to be sold by compute market tier.
     *
     * @param region the region based on which commodities sold are created.
     * @return The commodities to be sold by the marketTier traderTO
     */
    @Nonnull
    protected Collection<CommoditySoldTO> commoditiesSoldFromRegion(@Nonnull TopologyEntityDTO region) {
        Collection<CommoditySoldTO> commoditiesSold = new ArrayList<>();
        commoditiesSold.addAll(commodityConverter.commoditiesSoldList(region));
        return commoditiesSold;
    }

    /**
     * Create the compute tier commodities to be sold by compute market tier.
     *
     * @param computeTier the compute tier on which commodities sold are created.
     * @return The commodities to be sold by the marketTier traderTO
     */
    @Nonnull
    protected Collection<CommoditySoldTO> commoditiesSoldFromTier(@Nonnull final TopologyDTO.TopologyEntityDTO computeTier) {
        Collection<CommoditySoldTO> commoditiesSold = commodityConverter.commoditiesSoldList(computeTier);
        return commoditiesSold;
    }

}
