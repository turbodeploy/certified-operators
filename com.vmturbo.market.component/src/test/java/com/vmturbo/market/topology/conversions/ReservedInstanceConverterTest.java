package com.vmturbo.market.topology.conversions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.market.topology.conversions.ReservedInstanceAggregate.ReservedInstanceKey;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Unit tests for ReservedInstanceConverter.
 */
public class ReservedInstanceConverterTest {

    private ReservedInstanceConverter converter;

    /**
     * Initializes ReservedInstanceConverter instance.
     */
    @Before
    public void setUp() {
        final TopologyInfo info = TopologyInfo.newBuilder().build();
        final CommodityConverter commodityConverter =
                new CommodityConverter(new NumericIDAllocator(), new HashMap<>(), false,
                        new BiCliquer(), HashBasedTable.create(), new ConversionErrorCounts());
        final CostDTOCreator costDTOCreator = mock(CostDTOCreator.class);
        converter = new ReservedInstanceConverter(info, commodityConverter, costDTOCreator);
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

    private RiDiscountedMarketTier mockRiDiscountedTier(boolean platformFlexible) {
        final RiDiscountedMarketTier riDiscountedTier = mock(RiDiscountedMarketTier.class);
        final ReservedInstanceAggregate aggregate = mock(ReservedInstanceAggregate.class);
        final ReservedInstanceKey riKey = mock(ReservedInstanceKey.class);
        when(riKey.getOs()).thenReturn(OSType.LINUX);
        when(riKey.getTenancy()).thenReturn(Tenancy.DEFAULT);
        when(aggregate.getRiKey()).thenReturn(riKey);
        when(aggregate.isPlatformFlexible()).thenReturn(platformFlexible);
        when(riDiscountedTier.getRiAggregate()).thenReturn(aggregate);
        return riDiscountedTier;
    }

    private TopologyEntityDTO mockRegion() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(100L)
                .build();
    }

    private TopologyEntityDTO mockComputeTier() {
       return TopologyEntityDTO.newBuilder()
               .setEntityType(EntityType.COMPUTE_TIER_VALUE)
               .setOid(50L)
               .addAllCommoditySoldList(createComputeTierSoldCommodities())
               .build();
    }

    private List<CommoditySoldDTO> createComputeTierSoldCommodities() {
        return ImmutableList.of(createLicenseAccessCommoditySoldDTO("Linux"),
                        createLicenseAccessCommoditySoldDTO("Windows"),
                        createLicenseAccessCommoditySoldDTO("RHEL"));
    }

    private CommoditySoldDTO createLicenseAccessCommoditySoldDTO(final String platform) {
        return CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setKey(platform)
                        .build())
                .build();
    }
}