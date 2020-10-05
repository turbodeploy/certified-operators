package com.vmturbo.market.topology.conversions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
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
    private static final Float DELTA = 0.001f;

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
        converterToTest = new CommodityConverter(commodityTypeAllocator, includeGuaranteedBuyer,  dsBasedBicliquer,
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
        assertFalse(cpuCommodityTO.getSettings().hasResold());
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
        if (commodityTO.hasHistoricalQuantity()) {
            assertThat(commodityTO.getHistoricalQuantity(),
                    equalTo(PERCENTILE_USED * scalingFactor * RAW_CAPACITY));
        }
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

    /**
     * Tests creation of multiple commodity Specifications as in case of
     * Pool commodities that are time slot based.
     */
    @Test
    public void testCreateSpecifications() {

        CommodityType poolType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();
        Collection<CommoditySpecificationTO> specifications =
                converterToTest.commoditySpecification(poolType, 3);
        assertEquals(3, specifications.size());
        int index = 0;
        for (CommoditySpecificationTO spec: specifications) {
            index++;
            assertEquals(poolType.getType(), spec.getBaseType());
            final Optional<CommodityType> convertedType =
                    converterToTest.marketToTopologyCommodity(spec, Optional.of(index));
            assertTrue(convertedType.isPresent());
            assertEquals(convertedType.get(), poolType);
        }
    }

    /**
     * Tests creation of multiple commodity Specifications as in case of
     * Pool commodities that are time slot based.
     */
    @Test
    public void testCreatePoolSpecifications() {
        CommodityType poolType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();
        Collection<CommoditySpecificationTO> specifications =
            converterToTest.commoditySpecification(poolType, 3);
        assertEquals(3, specifications.size());
        int index = 0;
        for (CommoditySpecificationTO spec: specifications) {
            index++;
            assertEquals(poolType.getType(), spec.getBaseType());
            final Optional<CommodityType> convertedType =
                converterToTest.marketToTopologyCommodity(spec);
            assertTrue(convertedType.isPresent());
            assertEquals(convertedType.get(), poolType);
        }
    }

    /**
     * Tests creation of single commodity Specifications.
     */
    @Test
    public void testSingleSpecifications() {
        CommodityType vcpuType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE).build();
        CommoditySpecificationTO specification =
                converterToTest.commoditySpecification(vcpuType);
        assertEquals(vcpuType.getType(), specification.getBaseType());
        final Optional<CommodityType> convertedType =
                converterToTest.marketToTopologyCommodity(specification);
        assertTrue(convertedType.isPresent());
        assertEquals(convertedType.get(), vcpuType);
    }

    /**
     * Tests creation of multiple sold commodity TOs as in case of
     * Pool commodities that are time slot based.
     */
    @Test
    public void testCreateSplitSoldCommodities() {
        HistoricalValues histUsed = HistoricalValues.newBuilder()
                .setHistUtilization(0)
                .addTimeSlot(0d)
                .addTimeSlot(0d)
                .build();
        final TopologyEntityDTO originalTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(PM_OID)
                .setEntityType(EntityType.DESKTOP_POOL_VALUE)
                .addCommoditySoldList(createSoldCommodity(histUsed, SCALING_FACTOR,
                        CommodityDTO.CommodityType.POOL_CPU, 95))
                .build();
        final Collection<CommoditySoldTO> result =
                converterToTest.commoditiesSoldList(originalTopologyEntityDTO);
        // Assert
        assertThat(result.size(), equalTo(2));
        for (CommoditySoldTO to : result) {
            checkSoldCommodityTO(to, SCALING_FACTOR, RAW_CAPACITY * SCALING_FACTOR,
                    0f);
        }
    }

    /**
     * Test that the "isResold" flag gets correctly transferred to commSoldTO's when it
     * is present on the input CommodityDTO.
     */
    @Test
    public void testResoldCommodity() {
        final TopologyEntityDTO originalTopologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(PM_OID)
            .setEntityType(EntityType.DESKTOP_POOL_VALUE)
            .addCommoditySoldList(createSoldCommodity(HistoricalValues.getDefaultInstance(),
                SCALING_FACTOR, CommodityDTO.CommodityType.VCPU, 95)
                .toBuilder()
                .setIsResold(false))
            .addCommoditySoldList(createSoldCommodity(HistoricalValues.getDefaultInstance(),
                SCALING_FACTOR, CommodityDTO.CommodityType.VMEM, 95)
                .toBuilder()
                .setIsResold(true))
            .build();
        final Collection<CommoditySoldTO> result =
            converterToTest.commoditiesSoldList(originalTopologyEntityDTO);

        // Assert
        assertThat(result.size(), equalTo(2));
        assertThat(commSoldOfType(result, CommodityDTO.CommodityType.VCPU)
            .getSettings().getResold(), is(false));
        assertThat(commSoldOfType(result, CommodityDTO.CommodityType.VMEM)
            .getSettings().getResold(), is(true));
    }

    /**
     * Test get bought commodity historical utilization.
     */
    @Test
    public void testGetHistoricalUsed() {
        CommodityBoughtDTO commodityBoughtDTO = CommodityBoughtDTO.newBuilder()
            .setCommodityType(
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.POOL_CPU_VALUE)
                .build())
            .build();
        Optional<float[]> histUsed = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertFalse(histUsed.isPresent());
        HistoricalValues historicalValues =  HistoricalValues.newBuilder()
            .setPercentile(PERCENTILE_USED)
            .addAllTimeSlot(Arrays.asList(0.1d, 0.2d, 0.3d))
            .setHistUtilization(RAW_USED)
            .build();
        commodityBoughtDTO = commodityBoughtDTO.toBuilder().setHistoricalUsed(historicalValues)
            .build();
        histUsed = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertTrue(histUsed.isPresent());
        assertEquals(1, histUsed.get().length);
        assertEquals(PERCENTILE_USED, histUsed.get()[0], DELTA);
        commodityBoughtDTO = commodityBoughtDTO.toBuilder().setHistoricalUsed(
            historicalValues.toBuilder().clearPercentile().build()).build();
        histUsed = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertTrue(histUsed.isPresent());
        assertEquals(3, histUsed.get().length);
        commodityBoughtDTO = commodityBoughtDTO.toBuilder().setHistoricalUsed(
            historicalValues.toBuilder()
                .clearPercentile()
                .clearTimeSlot().build())
            .build();
        histUsed = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertTrue(histUsed.isPresent());
        assertEquals(1, histUsed.get().length);
        assertEquals(RAW_USED, histUsed.get()[0], DELTA);
    }

    /**
     * Test get bought commodity historical peak.
     */
    @Test
    public void testGetHistoricalPeak() {
        CommodityBoughtDTO commodityBoughtDTO = CommodityBoughtDTO.newBuilder()
            .setCommodityType(
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.POOL_CPU_VALUE)
                    .build())
            .build();
        Optional<float[]> histPeak = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalPeak);
        assertFalse(histPeak.isPresent());
        HistoricalValues historicalValues =  HistoricalValues.newBuilder()
            .setPercentile(PERCENTILE_USED)
            .addAllTimeSlot(Arrays.asList(0.1d, 0.2d, 0.3d))
            .setHistUtilization(RAW_USED)
            .build();
        commodityBoughtDTO = commodityBoughtDTO.toBuilder().setHistoricalUsed(historicalValues)
            .build();
        histPeak = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertTrue(histPeak.isPresent());
        assertEquals(1, histPeak.get().length);
        assertEquals(PERCENTILE_USED, histPeak.get()[0], DELTA);
        commodityBoughtDTO = commodityBoughtDTO.toBuilder().setHistoricalUsed(
            historicalValues.toBuilder().clearPercentile().build()).build();
        histPeak = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertTrue(histPeak.isPresent());
        assertEquals(3, histPeak.get().length);
        commodityBoughtDTO = commodityBoughtDTO.toBuilder().setHistoricalUsed(
            historicalValues.toBuilder()
                .clearPercentile()
                .clearTimeSlot().build())
            .build();
        histPeak = CommodityConverter.getHistoricalUsedOrPeak(commodityBoughtDTO,
            TopologyDTO.CommodityBoughtDTO::getHistoricalUsed);
        assertTrue(histPeak.isPresent());
        assertEquals(1, histPeak.get().length);
        assertEquals(RAW_USED, histPeak.get()[0], DELTA);
    }

    /**
     * Test remove consumer update provider utilization.
     */
    @Test
    public void testRemoveConsumerUpdateProviderUtilization() {
        final CommodityType commodityType = CommodityType.newBuilder().setType(10).build();
        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEntityState(EntityState.POWERED_ON)
            .setOid(3L)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(commodityType).setUsed(100).setScalingFactor(2))
            .build();

        final Map<Long, Map<CommodityType, Pair<Double, Double>>> input =
            ImmutableMap.of(pm.getOid(), ImmutableMap.of(commodityType, new Pair<>(30d, 30d)));
        converterToTest.setProviderUsedSubtractionMap(input);

        final List<CommoditySoldTO> commoditySoldTOs =
            converterToTest.createCommonCommoditySoldTOList(pm.getCommoditySoldList(0), pm);

        assertThat(commoditySoldTOs.get(0).getQuantity(), is(170f));
    }

    private CommoditySoldTO commSoldOfType(@Nonnull final Collection<CommoditySoldTO> commoditiesSold,
                                           @Nonnull final CommodityDTO.CommodityType type) {
        return commoditiesSold.stream()
            .filter(commSold -> commSold.getSpecification().getDebugInfoNeverUseInCode().contains(type.name()))
            .findFirst()
            .get();
    }
}
