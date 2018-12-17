package com.vmturbo.market.topology.conversions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test conversions from XL CommodityDTO to market CommodityTO
 **/
public class CommodityConverterTest {

    private static final long PM_OID = 1L;
    private static final Float SCALING_FACTOR = 1.5F;
    private static final Float RAW_USED = 0.5F;
    private static final Float RAW_CAPACITY = 2.0F;
    private static final Float MARKET_USED = RAW_USED * SCALING_FACTOR;
    private static final Float MARKET_CAPACITY = RAW_CAPACITY * SCALING_FACTOR;

    /**
     * Create a CommoditySoldTO (market) from a TopologyEntityDTO (xl).
     */
    @Test
    public void testCreateCommonCommoditySoldTO() {
        // Arrange
        NumericIDAllocator commodityTypeAllocator = new NumericIDAllocator();
        Map<String, CommodityType> commoditySpecMap = Mockito.mock(Map.class);
        boolean includeGuaranteedBuyer = false;
        BiCliquer dsBasedBicliquer = Mockito.mock(BiCliquer.class);
        Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable = Mockito.mock(Table.class);

        CommodityConverter converterToTest = new CommodityConverter(commodityTypeAllocator,
                commoditySpecMap, includeGuaranteedBuyer,  dsBasedBicliquer,
                numConsumersOfSoldCommTable);
        final TopologyEntityDTO originalTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(PM_OID)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCapacity(RAW_CAPACITY)
                        .setUsed(RAW_USED)
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                .build())
                        .setScalingFactor(SCALING_FACTOR)
                        .build())
                .build();
        // Act
        Collection<CommoditySoldTO> result = converterToTest.commoditiesSoldList(originalTopologyEntityDTO);
        // Assert
        assertThat(result.size(), equalTo(1));
        final CommoditySoldTO cpuCommodityTO = result.iterator().next();
        assertThat(cpuCommodityTO.getCapacity(), equalTo(MARKET_CAPACITY));
        assertThat(cpuCommodityTO.getQuantity(), equalTo(MARKET_USED));
    }
}
