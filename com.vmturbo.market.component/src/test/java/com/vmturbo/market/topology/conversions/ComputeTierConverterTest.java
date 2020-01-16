package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.FAMILY_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_ID;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.REGION_NAME;
import static com.vmturbo.market.topology.conversions.CloudTestEntityFactory.TIER_NAME;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.commons.analysis.NumericIDAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO.CommodityType;


/**
 * Unit tests for ComputeTierConverter.
 */
public class ComputeTierConverterTest {

    private ComputeTierConverter computeTierConverter;
    private CommodityConverter commodityConverter;

    /**
     * Initializes ComputeTierConverter instance.
     */
    @Before
    public void setUp() {
        IdentityGenerator.initPrefix(0);
        commodityConverter = new CommodityConverter(new NumericIDAllocator(), new HashMap<>(),
                false, new BiCliquer(), HashBasedTable.create(),
                new ConversionErrorCounts(), mock(ConsistentScalingHelper.class));
        final TopologyInfo info = TopologyInfo.newBuilder().build();
        final CostDTOCreator costDTOCreator = new CostDTOCreator(commodityConverter, null);
        computeTierConverter = new ComputeTierConverter(info, commodityConverter, costDTOCreator,
                mock(TierExcluder.class));
    }

    /**
     * Test that computeTier sells 3 TenancyAccess commodities. 1 for region, 1 for family and 1
     * for compute tier name.
     */
    @Test
    public void testTemplateAccessCommoditySold() {
        final TopologyEntityDTO computeTier = CloudTestEntityFactory.mockComputeTier();
        final TopologyEntityDTO region = CloudTestEntityFactory.mockRegion(REGION_ID, REGION_NAME);
        final Collection<CommoditySoldTO> soldTenancyAccessCommodities =
                computeTierConverter.commoditiesSoldList(computeTier, Collections.singletonList(region)).stream()
                .filter(c -> c.getSpecification().getBaseType()
                        == CommodityType.TEMPLATE_ACCESS.getValue())
                .collect(Collectors.toSet());
        verifyTemplateAccessCommodities(ImmutableSet.of(REGION_NAME, FAMILY_NAME, TIER_NAME),
                soldTenancyAccessCommodities);
    }

    private void verifyTemplateAccessCommodities(Set<String> expectedKeys,
                                                 Collection<CommoditySoldTO> soldCommodities) {
        final Set<String> commodityKeys = soldCommodities.stream()
                .map(c -> commodityConverter.commodityIdToCommodityType(c.getSpecification()
                        .getType()))
                .map(TopologyDTO.CommodityType::getKey)
                .collect(Collectors.toSet());
        Assert.assertEquals(expectedKeys, commodityKeys);
    }
}
