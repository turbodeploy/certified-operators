package com.vmturbo.market.topology.conversions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.PriceFunctionTypeCase;
import com.vmturbo.platform.analysis.utilities.BiCliquer;
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
    ConsistentScalingHelper consistentScalingHelper;
    @Before
    public void setup() {
        // Arrange
        commodityTypeAllocator = new NumericIDAllocator();
        commoditySpecMap = mock(Map.class);
        includeGuaranteedBuyer = false;
        dsBasedBicliquer = mock(BiCliquer.class);
        numConsumersOfSoldCommTable = mock(Table.class);
        consistentScalingHelper = new ConsistentScalingHelper(null);
        converterToTest = new CommodityConverter(commodityTypeAllocator,
                commoditySpecMap, includeGuaranteedBuyer,  dsBasedBicliquer,
                numConsumersOfSoldCommTable, new ConversionErrorCounts(), consistentScalingHelper);
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
                        .addCommoditySoldList(createSoldCommodity(histUsed, SCALING_FACTOR,
                                        CommodityDTO.CommodityType.CPU, effectiveCapacityPercentage))
                        .addCommoditySoldList(
                                        createSoldCommodity(histUsed, null,
                                                        CommodityDTO.CommodityType.MEM,
                                                        effectiveCapacityPercentage))
                        .build();
        // Act
        final Collection<CommoditySoldTO> result =
                        converterToTest.commoditiesSoldList(originalTopologyEntityDTO);
        // Assert
        assertThat(result.size(), equalTo(2));
        final CommoditySoldTO cpuCommodityTO =
                        getCommodityByType(result, CommodityDTO.CommodityType.CPU);
        checkSoldCommodityTO(cpuCommodityTO,
                        SCALING_FACTOR, MARKET_CAPACITY, MARKET_USED);
        checkSoldCommodityTO(getCommodityByType(result, CommodityDTO.CommodityType.MEM), null,
                        RAW_CAPACITY, RAW_USED);
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

    /**
     * Test that the resizable flag of comm sold TO created is based on entity state.
     * @param entityState the state of the entity
     * @param expectedResizable the expected resizable on the commodities sold
     */
    @Test
    @Parameters({"POWERED_ON, true", "POWERED_OFF, false", "SUSPENDED, false", "MAINTENANCE, false", "FAILOVER, false", "UNKNOWN, false"})
    @TestCaseName("Test #{index}: For EntityState {0}, the commodities sold are expected to be resizable {1}")
    public final void testCommSoldTOResizableIsBasedOnEntityState(EntityState entityState, boolean expectedResizable) {
        HistoricalValues histUsed = HistoricalValues.newBuilder()
            .setHistUtilization(RAW_USED)
            .setPercentile(PERCENTILE_USED)
            .build();
        final TopologyEntityDTO originalTopologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(PM_OID)
            .setEntityState(entityState)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(createSoldCommodity(histUsed, SCALING_FACTOR,
                CommodityDTO.CommodityType.VCPU, 100))
            .addCommoditySoldList(createSoldCommodity(histUsed, SCALING_FACTOR,
                CommodityDTO.CommodityType.VMEM, 100))
            .build();
        // Act
        final Collection<CommoditySoldTO> result =
            converterToTest.commoditiesSoldList(originalTopologyEntityDTO);
        // Assert
        assertThat(result.size(), equalTo(2));
        result.forEach(c -> assertEquals(expectedResizable, c.getSettings().getResizable()));
    }

    private static CommoditySoldTO getCommodityByType(Collection<CommoditySoldTO> commodities,
                    CommodityDTO.CommodityType desiredType) {
        return commodities.stream()
                        .filter(comm -> desiredType.getNumber() == comm.getSpecification()
                                        .getBaseType()).findAny().orElse(null);
    }

    private void checkSoldCommodityTO(CommoditySoldTO commodityTO, Float rawScalingFactor,
                    Float expectedCapacity, Float expectedUsed) {
        assertThat(commodityTO.getCapacity(), equalTo(expectedCapacity));
        assertThat(commodityTO.getQuantity(), equalTo(expectedUsed));
        final float scalingFactor = rawScalingFactor == null ? 1 : rawScalingFactor;
        assertThat(commodityTO.getHistoricalQuantity(),
                        equalTo(PERCENTILE_USED * scalingFactor * RAW_CAPACITY));
    }

    private CommoditySoldDTO createSoldCommodity(HistoricalValues histUsed, Float scalingFactor,
                    CommodityDTO.CommodityType commodityType, double effectiveCapacityPercentage) {
        final CommoditySoldDTO.Builder builder =
                        CommoditySoldDTO.newBuilder().setCapacity(RAW_CAPACITY)
                                        .setEffectiveCapacityPercentage(effectiveCapacityPercentage)
                                        .setHistoricalUsed(histUsed).setCommodityType(
                                        CommodityType.newBuilder()
                                                        .setType(commodityType.getNumber())
                                                        .build());
        if (scalingFactor != null) {
            builder.setScalingFactor(scalingFactor);
        }
        return builder.build();
    }

}
