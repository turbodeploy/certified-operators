package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE_WITH_KEY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_UNITS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.StatsAccumulator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class SoldCommoditiesInfoTest {
    static final TopologyEntityDTO PM_1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
            .setOid(1)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(COMMODITY_TYPE)
                    .setUsed(2)
                    .setPeak(3)
                    .setCapacity(4))
            .build();

    static final TopologyEntityDTO PM_2 = TopologyEntityDTO.newBuilder(PM_1)
            .setOid(2)
            .build();

    static final TopologyEntityDTO PM_3 = TopologyEntityDTO.newBuilder(PM_1)
            .setOid(3)
            .build();

    public static final StatValue TWO_VALUE_STAT = new StatsAccumulator()
        .record(4)
        .record(4)
        .toStatValue();

    @Test
    public void testEmpty() {
        SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .build();

        assertFalse(info.getCapacity(COMMODITY, 1L).isPresent());
        assertFalse(info.getAccumulatedRecords(COMMODITY, Collections.emptySet()).isPresent());
    }

    @Test
    public void testSoldCommoditiesCapacity() {

        SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .build();

        assertEquals(Double.valueOf(4), info.getCapacity(COMMODITY, 1L)
                .orElseThrow(() -> new RuntimeException("expected capacity")));
    }

    @Test
    public void testSoldCommoditiesWholeMarket() {

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        final StatRecord record = info.getAccumulatedRecords(COMMODITY, Collections.emptySet())
                .orElseThrow(() -> new RuntimeException("expected record"));

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(4).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .build();

        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testSoldCommoditiesEntities() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                // Adding extra entity that won't be included in the search.
                .addEntity(PM_3)
                .build();

        final StatRecord record =
                info.getAccumulatedRecords(COMMODITY, Sets.newHashSet(1L, 2L))
                        .orElseThrow(() -> new RuntimeException("expected record"));

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(4).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .build();
        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testSoldCommoditiesDuplicateDifferentKey() {

        // with a different key, both commodities are recorded
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // capacity is sum of the two capacities (4+11) added in testSameCommodities() below
                .setCapacity(new StatsAccumulator().record(7).record(4).toStatValue())
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // the average
                .setCurrentValue(3.5F)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3.5F).setMax(5).setMin(2).setTotal(7).build())
                .setValues(StatValue.newBuilder().setAvg(3.5F).setMax(5).setMin(2).setTotal(7).build())
                .setPeak(StatValue.newBuilder().setAvg(4.5F).setMax(6).setMin(3).setTotal(9).build())
                .build();
        testAddTwoCommodities(expectedStatRecord, COMMODITY_TYPE, COMMODITY_TYPE_WITH_KEY);
    }

    /**
     * In this test we add two commodies with the same CommodityType with the default key,
     * i.e. {type: "Mem", key: ""}.
     */
    @Test
    public void testSoldCommoditiesDuplicateNoKey() {

        // the values are from the first record persisted; the second record is ignored.
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setCapacity(StatsAccumulator.singleStatValue(4))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(2).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(2).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(3).build())
                .build();
        testAddTwoCommodities(expectedStatRecord, COMMODITY_TYPE, COMMODITY_TYPE);
    }

    /**
     * In this test we add two commodies with the same CommodityType with a specific key,
     * i.e. {type: "Mem", key: "key"}.
     */
    @Test
    public void testSoldCommoditiesDuplicateSameKey() {

        // the values are from the first record persisted; the second record is ignored.
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setCapacity(StatsAccumulator.singleStatValue(4))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(2).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(2).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(3).build())
                .build();
        testAddTwoCommodities(expectedStatRecord, COMMODITY_TYPE_WITH_KEY, COMMODITY_TYPE_WITH_KEY);
    }

    /**
     * Utility to add two commodity sold records and compare the result to the expected.
     * The values of the two records are defined here; the goal is to test behavior with
     * different CommodityType inputs for each of the two records.
     * <ul>
     * <li>Commodity Sold 1:  used=2, peak=3, capacity=4
     * <li>Commodity Sold 2:  used=5, peak=6, capacity=7
     * </ul>
     *
     * @param expectedStatRecord the expected result after adding the two commodities
     * @param commodityType1 the first CommodityType (type and key)
     * @param commodityType2 the second CommodityType (type and key)
     */
    private void testAddTwoCommodities(StatRecord expectedStatRecord, CommodityType commodityType1,
                                       CommodityType commodityType2) {
        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commodityType1)
                        .setUsed(2)
                        .setPeak(3)
                        .setCapacity(4))
                // The same commodity spec, different values. Expect match on the
                // latest value.
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commodityType2)
                        .setUsed(5)
                        .setPeak(6)
                        .setCapacity(7))
                .build();

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(pm)
                .build();

        final StatRecord record = info.getAccumulatedRecords(COMMODITY, Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("expected record"));

        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testSoldCommodityEntityNotFound() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        assertFalse(info.getAccumulatedRecords(COMMODITY, Sets.newHashSet(999L)).isPresent());
    }

    @Test
    public void testNotSoldCommodity() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        assertFalse(info.getAccumulatedRecords("beer", Sets.newHashSet(1L)).isPresent());
    }

    @Test
    public void testCommodityNotSoldByEntity() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                // Suppose some other entity sells CPU, but PM1 doesn't.
                .addEntity(TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1111)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU.getNumber())))
                        .build())
                .build();

        assertFalse(info.getAccumulatedRecords("CPU", Sets.newHashSet(PM_1.getOid())).isPresent());
    }
}
