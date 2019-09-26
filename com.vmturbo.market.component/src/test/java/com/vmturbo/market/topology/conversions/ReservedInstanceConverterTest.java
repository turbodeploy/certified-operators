package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.FAMILY_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.TIER_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.TIER_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.mockComputeTier;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.mockRegion;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.market.topology.conversions.ReservedInstanceAggregate.ReservedInstanceKey;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
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

    /**
     * Initializes ReservedInstanceConverter instance.
     */
    @Before
    public void setUp() {
        IdentityGenerator.initPrefix(0);
        final TopologyInfo info = TopologyInfo.newBuilder().build();
        commodityConverter = new CommodityConverter(new NumericIDAllocator(),
                new HashMap<>(), false, new BiCliquer(), HashBasedTable.create(),
                new ConversionErrorCounts());
        final CostDTOCreator costDTOCreator = mock(CostDTOCreator.class);
        final CostDTO cbtpCostDto = CostDTO.newBuilder().build();
        when(costDTOCreator.createCbtpCostDTO()).thenReturn(cbtpCostDto);
        converter = new ReservedInstanceConverter(info, commodityConverter, costDTOCreator, mock(TierExcluder.class));
    }

    /**
     * Test that non-platform flexible RI sells only 1 License Access commodity corresponding to the
     * RIs platform.
     */
    @Test
    public void testNonPlatformFlexibleLicenseCommodityConversion() {
        final TopologyEntityDTO computeTier = mockComputeTier();
        final RiDiscountedMarketTier riDiscountedTier = mockRiDiscountedTier(false);
        final TopologyEntityDTO region = mockRegion();
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
        final TopologyEntityDTO region = mockRegion();
        final Collection<CommoditySoldTO> licenseCommodityTOs =
                converter.commoditiesSoldList(computeTier, region, riDiscountedTier).stream()
                        .filter(c -> c.getSpecification().getBaseType()
                                == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .collect(Collectors.toList());

        Assert.assertEquals(3, licenseCommodityTOs.size());
    }

    /**
     * Test that non-ISF RIs have TemplateAccess commodity with key as the Compute Tier name.
     */
    @Test
    public void testNonIsfTemplateAccessCommodityConversion() {
        final List<TraderTO> traders = createMarketTierTraderTOs(false);
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
        final List<TraderTO> traders = createMarketTierTraderTOs(true);
        final TraderTO traderTO = traders.iterator().next();
        final List<CommodityBoughtTO> boughtTemplateAccessCommodities =
                extractCommodityOfType(traderTO, CommodityDTO.CommodityType.TEMPLATE_ACCESS_VALUE);

        verifyTemplateAccessCommodities(ImmutableSet.of(REGION_NAME, FAMILY_NAME),
                boughtTemplateAccessCommodities);
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

    private List<TraderTO> createMarketTierTraderTOs(boolean isf) {
        final CloudCostData cloudCostData = mock(CloudCostData.class);
        when(cloudCostData.getExistingRiBought())
                .thenReturn(Collections.singleton(createRiData(isf)));
        final Map<Long, TopologyEntityDTO> topology = ImmutableMap.of(REGION_ID, mockRegion(),
                TIER_ID, mockComputeTier());
        return converter.createMarketTierTraderTOs(cloudCostData,
                        topology, new HashSet<>()).keySet().stream().map(TraderTO.Builder::build)
                .collect(Collectors.toList());
    }

    private static ReservedInstanceData createRiData(boolean isf) {
        final OSType osType = isf ? OSType.LINUX : OSType.WINDOWS;
        final ReservedInstanceBought boughtRi = ReservedInstanceBought.newBuilder().build();
        final ReservedInstanceSpecInfo riInfo = ReservedInstanceSpecInfo.newBuilder()
                .setPlatformFlexible(false)
                .setOs(osType)
                .setSizeFlexible(isf)
                .setRegionId(REGION_ID)
                .setTierId(TIER_ID)
                .build();
        final ReservedInstanceSpec riSpec = ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(riInfo)
                .build();
        return new ReservedInstanceData(boughtRi, riSpec);
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
        when(riDiscountedTier.getRiAggregate()).thenReturn(aggregate);
        return riDiscountedTier;
    }
}