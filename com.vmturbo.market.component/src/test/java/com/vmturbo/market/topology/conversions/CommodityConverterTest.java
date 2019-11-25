package com.vmturbo.market.topology.conversions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.google.common.collect.Table;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.PriceFunctionTypeCase;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test conversions from XL CommodityDTO to market CommodityTO
 **/
@RunWith(JUnitParamsRunner.class)
public class CommodityConverterTest {

    private static final long PM_OID = 1L;
    private static final Float SCALING_FACTOR = 1.5F;
    private static final Float RAW_USED = 0.5F;
    private static final Float RAW_CAPACITY = 2.0F;
    private static final Float MARKET_USED = RAW_USED * SCALING_FACTOR;
    private static final Float MARKET_CAPACITY = RAW_CAPACITY * SCALING_FACTOR;
    private static final Float PERCENTILE_USED = 0.65F;

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
     * effectiveCapacityPercentage values:
     *  80 - utilizationUpperBound = 0.8. priceFunction scale = 1.0
     * 100 - utilizationUpperBound = 1.0. priceFunction scale = 1.0
     * 120 - utilizationUpperBound = 1.0. priceFunction scale = 1.2
     * @param effectiveCapacityPercentage represents utilizationUpperBound and priceFunction scale
     */
    @Test
    @Parameters({"100", "80", "120"})
    @TestCaseName("Test #{index}: (set|get)EffectiveCapacityPercentage({0})")
    public final void testCreateCommonCommoditySoldTO(double effectiveCapacityPercentage) {
        HistoricalValues histUsed = HistoricalValues.newBuilder()
                        .setHistUtilization(RAW_USED)
                        .setPercentile(PERCENTILE_USED)
                        .build();
        final TopologyEntityDTO originalTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(PM_OID)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCapacity(RAW_CAPACITY)
                        .setEffectiveCapacityPercentage(effectiveCapacityPercentage)
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
        assertThat(cpuCommodityTO.getHistoricalQuantity(),
                equalTo(PERCENTILE_USED * SCALING_FACTOR * RAW_CAPACITY));
        assertEquals(cpuCommodityTO.getSettings().getUtilizationUpperBound(),
                (effectiveCapacityPercentage / 100.0) > 1.0f ? 1.0f :
                        (effectiveCapacityPercentage / 100.0), .001f);
        assertEquals(cpuCommodityTO.getSettings().getPriceFunction().getPriceFunctionTypeCase(),
                PriceFunctionTypeCase.SCALED_CAPACITY_STANDARD_WEIGHTED);
        assertEquals(cpuCommodityTO.getSettings().getPriceFunction()
                        .getScaledCapacityStandardWeighted().getScale(),
                (effectiveCapacityPercentage / 100.0) < 1.0f ? 1.0f :
                        (effectiveCapacityPercentage / 100.0), 0.001f);

    }

}