package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.RiskBased;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.Average;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.UpdateCoupon;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * This class is used to create TraderTOs for the RIs bought
 *
 */
public class ReservedInstanceConverter extends ComputeTierConverter {
    private static final Logger logger = LogManager.getLogger();

    Map<Long, ReservedInstanceData> riDataMap = new HashMap<>();
    ReservedInstanceConverter(TopologyInfo topologyInfo, CommodityConverter commodityConverter,
                         @Nonnull CostDTOCreator costDTOCreator) {
        super(topologyInfo, commodityConverter, costDTOCreator);
    }

    public Map<TraderTO.Builder, MarketTier> createMarketTierTraderTOs(
            @Nonnull CloudCostData cloudCostData,
            @Nonnull Map<Long, TopologyEntityDTO> topology,
            @Nonnull Set<TopologyEntityDTO> businessAccounts) {

        ReservedInstanceAggregator aggregator = new ReservedInstanceAggregator(cloudCostData, topology);
        // create RI aggregates from the RIs bought
        Collection<ReservedInstanceAggregate> riAggregates = aggregator.aggregate();
        riDataMap = aggregator.getRIDataMap();

        Map<TraderTO.Builder, MarketTier> traderTOs = new HashMap<>();
        // iterate over all the aggregated RI objects and creates CBTPs out of them
        for (ReservedInstanceAggregate riAggregate : riAggregates) {
            TopologyEntityDTO computeTier = riAggregate.getLargestTier();
            TopologyEntityDTO region = topology.get(riAggregate.getRiKey().getRegionId());
            RiDiscountedMarketTier marketTier = new RiDiscountedMarketTier(computeTier, region, riAggregate);
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
                    .setIsEligibleForResizeDown(false)
                    .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                            .setRiskBased(RiskBased.newBuilder()
                                    .setCloudCost(
                                            CostDTO.newBuilder().setCbtpResourceBundle(
                                                    CbtpCostDTO.newBuilder().setCouponBaseType(
                                                            com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.COUPON_VALUE)
                                                            .setDiscountPercentage(1)
                                                    .build())
                                            .build())
                                    .build()))
                    .setQuoteFactor(1)
                    .build();

            TraderTO.Builder traderTOBuilder = EconomyDTOs.TraderTO.newBuilder()
                    // Type and Oid are the same in the topology DTOs and economy DTOs
                    .setOid(IdentityGenerator.next())
                    .setType(computeTier.getEntityType())
                    .setState(TopologyConversionUtils.traderState(computeTier))
                    .setSettings(settings)
                    .setTemplateForHeadroom(false)
                    .addShoppingLists(createShoppingListTO(riAggregate, topology))
                    .setDebugInfoNeverUseInCode(debugInfo)
                    .addAllCommoditiesSold(commoditiesSoldList(computeTier, region, marketTier));
            traderTOs.put(traderTOBuilder, marketTier);
        }
        return traderTOs;
    }

    /**
     * Create the commodities to be sold by CBTP market tier.
     *
     * @param computeTier the computeTier based on which commodities sold are created.
     * @param region the region based on which commodities sold are created.
     * @return The commodities to be sold by the marketTier traderTO
     */
    @Nonnull
    protected Collection<CommoditySoldTO> commoditiesSoldList(
            @Nonnull final TopologyDTO.TopologyEntityDTO computeTier,
            @Nonnull TopologyEntityDTO region,
            @Nonnull RiDiscountedMarketTier marketTier) {
        Collection<CommoditySoldTO> commoditiesSold = commodityConverter.commoditiesSoldList(computeTier);
        commoditiesSold.addAll(super.commoditiesSoldFromTier(computeTier, region));
        // create CouponComm
        float capacity = marketTier.getTotalNumberOfCouponsBought();
        float used = marketTier.getTotalNumberOfCouponsUsed();
        CommodityType commType =
                CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.COUPON_VALUE)
                            .build();
        UpdatingFunctionTO couponUf = UpdatingFunctionTO.newBuilder().setUpdateCoupon(
                UpdateCoupon.newBuilder().build()).build();
        commoditiesSold.add(commodityConverter.createCommoditySoldTO(commType, capacity, used,
                couponUf));

        // create TenancyAccess
        capacity = marketTier.getTotalNumberOfCouponsBought();
        used = 0;
        commType = CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.TENANCY_ACCESS_VALUE)
                            .setKey(marketTier.getRiAggregate().getRiKey().getTenancy().toString())
                            .build();
        UpdatingFunctionTO emptyUf = UpdatingFunctionTO.newBuilder().build();
        commoditiesSold.add(commodityConverter.createCommoditySoldTO(commType, capacity, used,
                emptyUf));
        return commoditiesSold;
    }

    /**
     * Create the shoppingListTO with the commodities bought by Discounted market tier.
     * currently it is the TemplateAccessComm's
     *
     * @param riAgg is the riAggregate representing the discountedMarketTier
     * @return The shoppingListTO with the commodityBought by the CBTP
     */
    private ShoppingListTO createShoppingListTO(ReservedInstanceAggregate riAgg,
            @Nonnull Map<Long, TopologyEntityDTO> topology) {
        List<CommodityBoughtTO> commBoughtList = new ArrayList<>();
        CommodityType familySpecificTACommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE)
                .setKey(riAgg.getRiKey().getFamily())
                .build();
        commBoughtList.add(CommodityBoughtTO.newBuilder()
                .setQuantity(0).setPeakQuantity(0)
                .setSpecification(commodityConverter
                        .commoditySpecification(familySpecificTACommType))
                .build());
        CommodityType regionSpecificTACommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE)
                .setKey(topology.get(riAgg.getRiKey().getRegionId()).getDisplayName())
                .build();
        commBoughtList.add(CommodityBoughtTO.newBuilder()
                .setQuantity(0).setPeakQuantity(0)
                .setSpecification(commodityConverter
                        .commoditySpecification(regionSpecificTACommType))
                .build());
        return ShoppingListTO.newBuilder()
                .setOid(IdentityGenerator.next())
                .setMovable(false)
                .addAllCommoditiesBought(commBoughtList).build();
    }

    public ReservedInstanceData getRiDataById(long riId) {
        return riDataMap.get(riId);
    }
}