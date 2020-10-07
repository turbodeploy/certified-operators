package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.FAMILY_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_ID_2;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_NAME_2;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.RI_BOUGHT_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.RI_BOUGHT_ID_2;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.TIER_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.TIER_ID_2;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.TIER_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.ZONE_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.mockComputeTier;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.mockRegion;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.pricing.ImmutableCoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Unit tests for ReservedInstanceConverter.
 */
public class ReservedInstanceConverterTest {

    private ReservedInstanceConverter converter;
    private CommodityConverter commodityConverter;

    private static final int couponCapacity = 8;
    private static final double delta = 0.001;
    private static final long accountPricingOid = 1234L;
    private static final long businessAccountOid = 5678L;
    private static final long businessAccount2Oid = 7890L;
    private final Map<Long, TopologyEntityDTO> topology = ImmutableMap.of(
            REGION_ID, mockRegion(REGION_ID, REGION_NAME),
            REGION_ID_2, mockRegion(REGION_ID_2, REGION_NAME_2),
            TIER_ID, mockComputeTier(),
            TIER_ID_2, mockComputeTier(TIER_ID_2, false));

    private AccountPricingData accountPricingData = mock(AccountPricingData.class);
    private CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);
    private Map<Long, AccountPricingData> accountPricingDataByBusinessAccountMap;
    private Set<CoreBasedLicensePriceBundle> reservedLicenseBundle = ImmutableSet.of(
            ImmutableCoreBasedLicensePriceBundle
                    .builder()
                    .licenseCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                            .build())
                    .isBurstableCPU(false)
                    .osType(OSType.WINDOWS_BYOL)
                    .numCores(1)
                    .price(.003)
                    .build(),
            ImmutableCoreBasedLicensePriceBundle
                    .builder()
                    .licenseCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                            .build())
                    .isBurstableCPU(false)
                    .osType(OSType.LINUX_WITH_SQL_ENTERPRISE)
                    .numCores(1)
                    .price(0.001)
                    .build(),
            ImmutableCoreBasedLicensePriceBundle
                    .builder()
                    .licenseCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                            .build())
                    .isBurstableCPU(false)
                    .osType(OSType.WINDOWS)
                    .numCores(1)
                    .price(0.009)
                    .build(),
            ImmutableCoreBasedLicensePriceBundle
                    .builder()
                    .licenseCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                            .build())
                    .isBurstableCPU(false)
                    .osType(OSType.RHEL)
                    .numCores(1)
                    .price(0.011)
                    .build());

    /**
     * Initializes ReservedInstanceConverter instance.
     */
    @Before
    public void setUp() {
        IdentityGenerator.initPrefix(0);
        final TopologyInfo info = TopologyInfo.newBuilder().build();
        commodityConverter = new CommodityConverter(new NumericIDAllocator(),
                false, new BiCliquer(), HashBasedTable.create(),
                new ConversionErrorCounts(), mock(ConsistentScalingHelper.class));
        final CostDTOCreator costDTOCreator = new CostDTOCreator(commodityConverter,
                marketCloudRateExtractor);
        final CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopologyMock();
        converter = new ReservedInstanceConverter(info, commodityConverter, costDTOCreator,
                mock(TierExcluder.class), cloudTopology);
        when(marketCloudRateExtractor.getComputePriceBundle(any(), anyLong(), any()))
                .thenReturn(ComputePriceBundle.newBuilder().build());
        when(marketCloudRateExtractor.getReservedLicensePriceBundles(any(), any()))
                .thenReturn(Collections.singleton(ImmutableCoreBasedLicensePriceBundle.builder()
                        .licenseCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                            .build())
                        .isBurstableCPU(false)
                        .osType(OSType.LINUX)
                        .numCores(1)
                        .build()));
        accountPricingDataByBusinessAccountMap = new HashMap<>();
        accountPricingDataByBusinessAccountMap.put(businessAccountOid, accountPricingData);
        accountPricingDataByBusinessAccountMap.put(businessAccount2Oid, accountPricingData);
    }

    private CloudTopology<TopologyEntityDTO> createCloudTopologyMock() {
        final CloudTopology<TopologyEntityDTO> cloudTopology =
                mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getBillingFamilyForEntity(businessAccountOid))
                .thenReturn(Optional.of(ImmutableGroupAndMembers.builder()
                        .group(Grouping.newBuilder()
                                .setId(11111L)
                                .build())
                        .members(ImmutableSet.of(businessAccountOid, businessAccount2Oid))
                        .entities(ImmutableSet.of(businessAccountOid, businessAccount2Oid))
                        .build()));
        return cloudTopology;
    }

    /**
     * Test that non-platform flexible RI sells only 1 License Access commodity corresponding to the
     * RIs platform.
     */
    @Test
    public void testNonPlatformFlexibleLicenseCommodityConversion() {
        final TopologyEntityDTO computeTier = mockComputeTier();
        final RiDiscountedMarketTier riDiscountedTier = mockRiDiscountedTier(false);
        final TopologyEntityDTO region = mockRegion(REGION_ID, REGION_NAME);
        final Collection<CommoditySoldTO> licenseCommodityTOs =
                converter.commoditiesSoldList(computeTier, region, riDiscountedTier).stream()
                .filter(c -> c.getSpecification().getBaseType()
                        == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .collect(Collectors.toList());

        Assert.assertEquals(1, licenseCommodityTOs.size());
    }

    /**
     * Test that platform flexible RI sells all License Access commodities sold by the compute tier.
     */
    @Test
    public void testPlatformFlexibleLicenseCommodityConversion() {
        final TopologyEntityDTO computeTier = mockComputeTier();
        final RiDiscountedMarketTier riDiscountedTier = mockRiDiscountedTier(true);
        final TopologyEntityDTO region = mockRegion(REGION_ID, REGION_NAME);
        final Collection<CommoditySoldTO> licenseCommodityTOs =
                converter.commoditiesSoldList(computeTier, region, riDiscountedTier).stream()
                        .filter(c -> c.getSpecification().getBaseType()
                                == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .collect(Collectors.toList());

        Assert.assertEquals(3, licenseCommodityTOs.size());
    }

    /**
     * Test that the tenancy commodity sold by the RI trader is based on the tenancy of the RI,
     * and not based on the tenancy of the compute tier.
     * Compute tier sells dedicated and host tenancies. But the RI is default tenancy. We want the
     * RI trader to have default tenancy access commodity.
     */
    @Test
    public void testTenancyCommodityConversion() {
        final TopologyEntityDTO computeTier = mockComputeTier();
        final RiDiscountedMarketTier riDiscountedTier = mockRiDiscountedTier(true);
        final TopologyEntityDTO region = mockRegion(REGION_ID, REGION_NAME);
        final List<CommoditySoldTO> tenancyCommodityTOs =
            converter.commoditiesSoldList(computeTier, region, riDiscountedTier).stream()
                .filter(c -> c.getSpecification().getBaseType()
                    == CommodityDTO.CommodityType.TENANCY_ACCESS_VALUE)
                .collect(Collectors.toList());

        Assert.assertEquals(1, tenancyCommodityTOs.size());
        Assert.assertEquals("TENANCY_ACCESS|DEFAULT", tenancyCommodityTOs.get(0).getSpecification().getDebugInfoNeverUseInCode());
    }

    /**
     * Test that non-ISF RIs have TemplateAccess commodity with key as the Compute Tier name.
     */
    @Test
    public void testNonIsfTemplateAccessCommodityConversion() {
        final List<TraderTO> traders = createMarketTierTraderTOs(false, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final List<CommodityBoughtTO> boughtTemplateAccessCommodities =
                extractCommodityOfType(traderTO, CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE);

        verifyTemplateAccessCommodities(ImmutableSet.of(REGION_NAME, TIER_NAME),
                boughtTemplateAccessCommodities);
    }

    /**
     * Test that ISF RIs have TemplateAccess commodity with key as the Compute Tier family name.
     */
    @Test
    public void testIsfTemplateAccessCommodityConversion() {
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, true);
        final TraderTO traderTO = traders.iterator().next();
        final List<CommodityBoughtTO> boughtTemplateAccessCommodities =
                extractCommodityOfType(traderTO, CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE);

        verifyTemplateAccessCommodities(ImmutableSet.of(REGION_NAME, FAMILY_NAME),
                boughtTemplateAccessCommodities);
    }

    /**
     * Test that the CostTuple for a Zonal RI Trader has zoneId set and regionId unset.
     */
    @Test
    public void testZoneInformationInCbtpCostDto() {
        final List<TraderTO> traders = createMarketTierTraderTOs(true, true, false);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO =
                traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        Assert.assertTrue(costDTO.hasCbtpResourceBundle());
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertFalse(cbtpCostDTO.getCostTupleListList().isEmpty());
        final CostTuple costTuple = cbtpCostDTO.getCostTupleListList().iterator().next();
        Assert.assertEquals(ZONE_ID, costTuple.getZoneId());
        Assert.assertFalse(costTuple.hasRegionId());
    }

    /**
     * Test that the CostTuple for a Regional RI Trader has regionId set and zoneId unset.
     */
    @Test
    public void testRegionInformationInCbtpCostDto() {
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO =
                traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        Assert.assertTrue(costDTO.hasCbtpResourceBundle());
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertFalse(cbtpCostDTO.getCostTupleListList().isEmpty());
        final CostTuple costTuple = cbtpCostDTO.getCostTupleListList().iterator().next();
        Assert.assertEquals(REGION_ID, costTuple.getRegionId());
        Assert.assertFalse(costTuple.hasZoneId());
    }

    /**
     * Test that single scope CBTPs have the business account id set in the cost DTO.
     */
    @Test
    public void testNonSharedAccountScopesSetInCbtpCostDto() {
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO =
                traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertEquals(1, cbtpCostDTO.getCostTupleListList().size());
        Assert.assertEquals(businessAccountOid, cbtpCostDTO.getScopeIdsList().iterator().next().longValue());
    }

    /**
     * Test that shared scope CBTPs have the price id set in the cost tuples.
     */
    @Test
    public void testSharedAccountScopeSetInCbtpCostDto() {
        final List<ReservedInstanceData> riDataList = Collections.singletonList(
                createRiData(true, false, TIER_ID, REGION_ID, 0L, RI_BOUGHT_ID, true));
        final List<TraderTO> traders = createMarketTierTraderTOs(riDataList);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO =
                traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertEquals(1, cbtpCostDTO.getCostTupleListList().size());
        final CostTuple costTuple = cbtpCostDTO.getCostTupleListList().iterator().next();
        Assert.assertEquals((long)accountPricingData.getAccountPricingDataOid(),
                costTuple.getBusinessAccountId());
    }

    /**
     * Test the case where no reserved license pricing is present. The RI discounted market tier
     * should have the fractional RI pricing set in the cost dto.
     */
    @Test
    public void testFractionalRICostInCbtpCostDtoWithoutReservedLicensePricing() {
        ComputePriceBundle bundle = ComputePriceBundle.newBuilder().addPrice(accountPricingOid, OSType.LINUX, 0.007, true).build();
        when(marketCloudRateExtractor.getComputePriceBundle(mockComputeTier(), REGION_ID, accountPricingData)).thenReturn(bundle);
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO = traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        Assert.assertTrue(costDTO.hasCbtpResourceBundle());
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertFalse(cbtpCostDTO.getCostTupleListList().isEmpty());
        final CostTuple costTuple = cbtpCostDTO.getCostTupleListList().iterator().next();
        Assert.assertEquals(7.0E-8, costTuple.getPrice(), 10^-9);
        accountPricingDataByBusinessAccountMap.clear();
    }

    /**
     * Test the case where both Reserved License Pricing is present and no fractional RI pricing is present.
     * This is an edge use case as there will be very few times when compute pricing is not present and
     * reserved license pricing is.
     */
    @Test
    public void testRIPricingInCbtpCostDTOWithReservedLicensePricing() {
        when(marketCloudRateExtractor.getReservedLicensePriceBundles(accountPricingData, mockComputeTier()))
                .thenReturn(reservedLicenseBundle);
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO = traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        Assert.assertTrue(costDTO.hasCbtpResourceBundle());
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertFalse(cbtpCostDTO.getCostTupleListList().isEmpty());
        Assert.assertEquals(cbtpCostDTO.getCostTupleListList().size(), 5);
        Optional<CostTuple> costTuple = cbtpCostDTO.getCostTupleListList().stream().filter(s -> s.getLicenseCommodityType() == 1).findFirst();
        accountPricingDataByBusinessAccountMap.clear();
    }

    /**
     * Test the case where both Reserved license pricing and fractional RI pricing are present.
     * The reserved license pricing should be used instead of the fractional RI pricing.
     */
    @Test
    public void testRIPricingInCbtpCostDTOWithReservedLicenseandFractionalPricing() {
        when(marketCloudRateExtractor.getReservedLicensePriceBundles(accountPricingData, mockComputeTier()))
                .thenReturn(reservedLicenseBundle);
        ComputePriceBundle bundle = ComputePriceBundle.newBuilder().addPrice(accountPricingOid, OSType.LINUX, 0.007, true).build();
        when(marketCloudRateExtractor.getComputePriceBundle(mockComputeTier(), REGION_ID, accountPricingData)).thenReturn(bundle);
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final CostDTO costDTO = traderTO.getSettings().getQuoteFunction().getRiskBased().getCloudCost();
        Assert.assertTrue(costDTO.hasCbtpResourceBundle());
        final CbtpCostDTO cbtpCostDTO = costDTO.getCbtpResourceBundle();
        Assert.assertFalse(cbtpCostDTO.getCostTupleListList().isEmpty());

    }

    /**
     * Test that coupon commodity sold used value is set correctly.
     */
    @Test
    public void testCouponCommoditySold() {
        final List<TraderTO> traders = createMarketTierTraderTOs(true, false, false);
        final TraderTO traderTO = traders.iterator().next();
        final Optional<CommoditySoldTO> soldCouponCommodity =
                traderTO.getCommoditiesSoldList().stream().filter(c ->
                c.getSpecification().getBaseType() == CommodityDTO.CommodityType.COUPON_VALUE)
                .findAny();
        Assert.assertTrue(soldCouponCommodity.isPresent());
        final CommoditySoldTO couponCommodity = soldCouponCommodity.get();
        Assert.assertEquals(couponCapacity, couponCommodity.getCapacity(), delta);
        Assert.assertEquals(couponCapacity, couponCommodity.getQuantity(), delta);
    }

    private void verifyTemplateAccessCommodities(Set<String> expectedKeys,
                                                 List<CommodityBoughtTO> boughtCommodities) {
        final Set<String> commodityKeys = boughtCommodities.stream()
                .map(c -> commodityConverter.commodityIdToCommodityType(c.getSpecification()
                        .getType()))
                .map(CommodityType::getKey)
                .collect(Collectors.toSet());
        Assert.assertEquals(expectedKeys, commodityKeys);
    }

    private static List<CommodityBoughtTO> extractCommodityOfType(TraderTO trader, int type) {
        return trader.getShoppingListsList().iterator().next().getCommoditiesBoughtList()
                .stream()
                .filter(c -> c.getSpecification().getBaseType() == type)
                .collect(Collectors.toList());
    }

    private List<TraderTO> createMarketTierTraderTOs(boolean isf, boolean zonal,
                                                     boolean includeRiWithBadRegion) {
        final CloudCostData cloudCostData = mock(CloudCostData.class);
        final List<ReservedInstanceData> riDataList = new ArrayList<>();
        riDataList.add(createRiData(isf, zonal, TIER_ID, REGION_ID, ZONE_ID, RI_BOUGHT_ID, false));
        if (includeRiWithBadRegion) {
            riDataList.add(createRiData(isf, zonal, TIER_ID_2, REGION_ID_2, 0L, RI_BOUGHT_ID_2,
                    false));
        }
        when(cloudCostData.getExistingRiBought()).thenReturn(riDataList);
        when(cloudCostData.getFilteredRiCoverageByEntityId()).thenReturn(getRiCoverageMap());
        final Map<Long, TopologyEntityDTO> topology = ImmutableMap.of(
                REGION_ID, mockRegion(REGION_ID, REGION_NAME),
                REGION_ID_2, mockRegion(REGION_ID_2, REGION_NAME_2),
                TIER_ID, mockComputeTier(),
                TIER_ID_2, mockComputeTier(TIER_ID_2, false));
        return converter.createMarketTierTraderTOs(cloudCostData,
                        topology, accountPricingDataByBusinessAccountMap).keySet().stream()
                .map(TraderTO.Builder::build)
                .collect(Collectors.toList());
    }

    private List<TraderTO> createMarketTierTraderTOs(final List<ReservedInstanceData> riDataList) {
        final CloudCostData cloudCostData = mock(CloudCostData.class);
        when(cloudCostData.getExistingRiBought()).thenReturn(riDataList);
        when(cloudCostData.getCurrentRiCoverage()).thenReturn(getRiCoverageMap());
        return converter.createMarketTierTraderTOs(cloudCostData,
                topology, accountPricingDataByBusinessAccountMap).keySet().stream()
                .map(TraderTO.Builder::build)
                .collect(Collectors.toList());
    }

    private Map<Long, EntityReservedInstanceCoverage> getRiCoverageMap() {
        return ImmutableMap.of(123L, EntityReservedInstanceCoverage
                .newBuilder().putCouponsCoveredByRi(RI_BOUGHT_ID, 4).build(), 234L,
                EntityReservedInstanceCoverage.newBuilder().putCouponsCoveredByRi(RI_BOUGHT_ID,
                        4).build());
    }

    private static ReservedInstanceData createRiData(boolean isf, boolean zonal, long tierId,
                                                     long regionId, long zoneId, long boughtId,
                                                     boolean sharedScope) {
        final OSType osType = isf ? OSType.LINUX : OSType.WINDOWS;
        final ReservedInstanceBought.Builder boughtRiBuilder = ReservedInstanceBought
                .newBuilder().setId(boughtId);
        final ReservedInstanceBoughtInfo.Builder boughtInfoBuilder =
                ReservedInstanceBoughtInfo.newBuilder();
        if (zonal) {
            boughtInfoBuilder.setAvailabilityZoneId(zoneId);
        }
        boughtInfoBuilder.setBusinessAccountId(businessAccountOid);
        boughtInfoBuilder.setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons
                .newBuilder().setNumberOfCoupons(couponCapacity).build());
        final ReservedInstanceScopeInfo.Builder scopeInfoBuilder =
                ReservedInstanceScopeInfo.newBuilder();
        if (sharedScope) {
            scopeInfoBuilder.setShared(true)
                    .addAllApplicableBusinessAccountId(ImmutableSet.of(businessAccountOid,
                            businessAccount2Oid));
        } else {
            scopeInfoBuilder.setShared(false)
                    .addApplicableBusinessAccountId(businessAccountOid);
        }
        boughtInfoBuilder.setReservedInstanceScopeInfo(scopeInfoBuilder);
        boughtRiBuilder.setReservedInstanceBoughtInfo(boughtInfoBuilder.build());
        final ReservedInstanceSpecInfo riInfo = ReservedInstanceSpecInfo.newBuilder()
                .setPlatformFlexible(true)
                .setOs(osType)
                .setSizeFlexible(isf)
                .setRegionId(regionId)
                .setTierId(tierId)
                .build();
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(riInfo)
                .build();
        return new ReservedInstanceData(boughtRiBuilder.build(), riSpec);
    }

    private static RiDiscountedMarketTier mockRiDiscountedTier(boolean platformFlexible) {
        final RiDiscountedMarketTier riDiscountedTier = mock(RiDiscountedMarketTier.class);
        final ReservedInstanceAggregate aggregate = mock(ReservedInstanceAggregate.class);
        final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(EntityType.COMPUTE_TIER.getValue()).build();
        final ReservedInstanceKey riKey = mock(ReservedInstanceKey.class);
        when(riKey.getOs()).thenReturn(OSType.LINUX);
        when(riKey.getTenancy()).thenReturn(Tenancy.DEFAULT);
        when(aggregate.getRiKey()).thenReturn(riKey);
        when(aggregate.isPlatformFlexible()).thenReturn(platformFlexible);
        when(aggregate.getComputeTier()).thenReturn(computeTier);
        when(aggregate.getComputeTiersInScope()).thenReturn(Collections.singleton(computeTier));
        when(riDiscountedTier.getRiAggregate()).thenReturn(aggregate);
        return riDiscountedTier;
    }
}
