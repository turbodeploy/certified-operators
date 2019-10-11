package com.vmturbo.market.topology.conversions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.commons.analysis.AnalysisUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
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

    NumericIDAllocator commodityTypeAllocator;
    Map<String, CommodityType> commoditySpecMap;
    boolean includeGuaranteedBuyer;
    BiCliquer dsBasedBicliquer;
    Table<Long, CommodityType, Integer> numConsumersOfSoldCommTable;
    CommodityConverter converterToTest;
    @Before
    public void setup() {
        // Arrange
        commodityTypeAllocator = new NumericIDAllocator();
        commoditySpecMap = Mockito.mock(Map.class);
        includeGuaranteedBuyer = false;
        dsBasedBicliquer = Mockito.mock(BiCliquer.class);
        numConsumersOfSoldCommTable = Mockito.mock(Table.class);
        converterToTest = new CommodityConverter(commodityTypeAllocator,
                commoditySpecMap, includeGuaranteedBuyer,  dsBasedBicliquer,
                numConsumersOfSoldCommTable, new ConversionErrorCounts());
    }

    /**
     * Create a CommoditySoldTO (market) from a TopologyEntityDTO (xl).
     */
    @Test
    public void testCreateCommonCommoditySoldTO() {
        HistoricalValues histUsed = HistoricalValues.newBuilder()
                        .setHistUtilization(RAW_USED)
                        .build();
        final TopologyEntityDTO originalTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(PM_OID)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCapacity(RAW_CAPACITY)
                        .setHistoricalUsed(histUsed)
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

    @Test
    public void testPopulateCommToConsiderForOverheadMap() {
        long context = 1;
        // setting up for plan1
        TopologyDTO.TopologyInfo topologyInfo = TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyContextId(context).build();
        converterToTest.populateCommToConsiderForOverheadMap(topologyInfo);

        // setting up for plan2
        topologyInfo.toBuilder().setTopologyContextId(2).build();
        converterToTest.populateCommToConsiderForOverheadMap(topologyInfo);

        // assert that there are 2 entries for the 2 plans
        assertThat(converterToTest.commToConsiderForOverheadMap.size(),
                equalTo(2));

        // assert the presence of commsToSkip for the 2nd plan
        assertThat(converterToTest.commToConsiderForOverheadMap.get(context).size(),
                equalTo(AnalysisUtil.COMM_TYPES_TO_ALLOW_OVERHEAD.size()));
    }
}
