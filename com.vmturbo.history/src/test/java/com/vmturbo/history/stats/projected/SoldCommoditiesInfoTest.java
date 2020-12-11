package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE_WITH_KEY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_UNITS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.utils.HistoryStatsUtils;
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
        SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .build();

        assertFalse(info.getCapacity(COMMODITY, 1L).isPresent());
        assertTrue(info.getAccumulatedRecords(COMMODITY, Collections.emptySet()).isEmpty());
    }

    @Test
    public void testSoldCommoditiesEntities2() {
        Collection<CommoditySoldDTO> soldDTOS = new ArrayList<>();

        CommoditySoldDTO soldDto1 = CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CONNECTION_VALUE)
                        .setKey("jdbc/sqlserver")
                        .build())
                .setUsed(1)
                .setPeak(2)
                .setCapacity(3)
                .build();

        CommoditySoldDTO soldDto2 = CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CONNECTION_VALUE)
                        .setKey("jdbc/mysql")
                        .build())
                .setUsed(4)
                .setPeak(5)
                .setCapacity(6)
                .build();

        soldDTOS.add(soldDto1);
        soldDTOS.add(soldDto2);
        TopologyEntityDTO PM_1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setOid(1)
                .addAllCommoditySoldList(soldDTOS)
                .build();

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .build();

        final List<StatRecord>
                records = info.getAccumulatedRecords("Connection", Sets.newHashSet(1L, 2L));
        records.get(0).getCapacity();
    }

    @Test
    public void testSoldCommoditiesCapacity() {

        SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(
                Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .build();

        assertEquals(Double.valueOf(4), info.getCapacity(COMMODITY, 1L)
                .orElseThrow(() -> new RuntimeException("expected capacity")));
    }

    @Test
    public void testSoldCommoditiesWholeMarket() {

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        final List<StatRecord> records = info.getAccumulatedRecords(COMMODITY, Collections.emptySet());

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .build();

        assertEquals(expectedStatRecord, records.get(0));
    }

    @Test
    public void testSoldCommoditiesEntities() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .addEntity(PM_2)
                // Adding extra entity that won't be included in the search.
                .addEntity(PM_3)
                .build();

        final List<StatRecord> records =
                info.getAccumulatedRecords(COMMODITY, Sets.newHashSet(1L, 2L));

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .build();
        assertEquals(expectedStatRecord, records.get(0));
    }

    @Test
    public void testSoldCommoditiesDuplicateDifferentKey() {

        final StatRecord expectedStatRecordWithKey = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setStatKey(COMMODITY)
                .setCapacity(new StatsAccumulator().record(7).toStatValue())
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // the average
                .setCurrentValue(5.0F)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(5).setMax(6).setMin(5).setTotal(5).setTotalMax(6).setTotalMin(5).build())
                .setValues(StatValue.newBuilder().setAvg(5).setMax(6).setMin(5).setTotal(5).setTotalMax(6).setTotalMin(5).build())
                .setPeak(StatValue.newBuilder().setAvg(5).setMax(6).setMin(5).setTotal(5).setTotalMax(6).setTotalMin(5).build())
                .build();

        final StatRecord expectedStatRecordWithoutKey = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setCapacity(new StatsAccumulator().record(4).toStatValue())
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // the average
                .setCurrentValue(2.0F)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .build();

        List<StatRecord> statRecords =
                testAddTwoCommodities(COMMODITY_TYPE, COMMODITY_TYPE_WITH_KEY);
        assertEquals(2, statRecords.size());
        if (statRecords.get(0).hasStatKey()) {
            assertEquals(expectedStatRecordWithKey, statRecords.get(0));
            assertEquals(expectedStatRecordWithoutKey, statRecords.get(1));
        } else {
            assertEquals(expectedStatRecordWithKey, statRecords.get(1));
            assertEquals(expectedStatRecordWithoutKey, statRecords.get(0));
        }
    }

    /**
     * In this test we add two commodities with the same CommodityType with the default key,
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
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .build();
        List<StatRecord> statRecords = testAddTwoCommodities(COMMODITY_TYPE, COMMODITY_TYPE);
        assertEquals(1, statRecords.size());
        assertEquals(expectedStatRecord, statRecords.get(0));
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
                .setStatKey(COMMODITY)
                .setCapacity(StatsAccumulator.singleStatValue(4))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .build();
        List<StatRecord> statRecords = testAddTwoCommodities(COMMODITY_TYPE_WITH_KEY, COMMODITY_TYPE_WITH_KEY);
        assertEquals(1, statRecords.size());
        assertEquals(expectedStatRecord, statRecords.get(0));
    }

    /**
     * Utility to add two commodity sold records and compare the result to the expected.
     * The values of the two records are defined here; the goal is to test behavior with
     * different CommodityType inputs for each of the two records.
     * <ul>
     * <li>Commodity Sold 1:  used=2, peak=3, capacity=4
     * <li>Commodity Sold 2:  used=5, peak=6, capacity=7
     * </ul>
     * @param commodityType1 the first CommodityType (type and key)
     * @param commodityType2 the second CommodityType (type and key)
     * @return list of stat records
     */
    private List<StatRecord> testAddTwoCommodities(CommodityType commodityType1,
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

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(pm)
                .build();

        return info.getAccumulatedRecords(COMMODITY, Collections.emptySet());
    }

    @Test
    public void testSoldCommodityEntityNotFound() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        assertTrue(info.getAccumulatedRecords(COMMODITY, Sets.newHashSet(999L)).isEmpty());
    }

    /**
     * Test that excluded commodities do not make it into the {@link SoldCommoditiesInfo}.
     */
    @Test
    public void testSoldCommodityExclusion() {
        final CommodityDTO.CommodityType commType = CommodityDTO.CommodityType.CLUSTER;
        final String commodityName = HistoryStatsUtils.formatCommodityName(commType.getNumber());

        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.CLUSTER_VALUE))
                        .setUsed(2)
                        .setPeak(3)
                        .setCapacity(4))
                .build();

        final SoldCommoditiesInfo noExclusionInfo = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(pm)
                .build();
        assertThat(noExclusionInfo.getValue(pm.getOid(), commodityName), is(2.0));

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.singleton(commType),
                new LongDataPack(), new DataPack<>())
                .addEntity(pm)
                .build();
        assertThat(info.getValue(pm.getOid(), commodityName), is(0.0));
    }

    @Test
    public void testNotSoldCommodity() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        assertTrue(info.getAccumulatedRecords("beer", Sets.newHashSet(1L)).isEmpty());
    }

    @Test
    public void testCommodityNotSoldByEntity() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
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

        assertTrue(info.getAccumulatedRecords("CPU", Sets.newHashSet(PM_1.getOid())).isEmpty());
    }

    @Test
    public void testSoldCommodityGetValue() {
        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE)
                        .setUsed(2))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE_WITH_KEY)
                        .setUsed(8))
                .build();
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(pm)
                .build();

        // Because there are two commodities of the same type with different keys, we should
        // get the average of the two used values.
        assertThat(info.getValue(pm.getOid(), COMMODITY), is(5.0));
    }

    @Test
    public void testSoldCommodityGetValueNoCommodity() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .build();

        assertThat(info.getValue(PM_1.getOid(), "beer"), is(0.0));
    }

    @Test
    public void testSoldCommodityGetValueEntityNotFound() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder(Collections.emptySet(),
                new LongDataPack(), new DataPack<>())
                .addEntity(PM_1)
                .build();

        assertThat(info.getValue(123L, COMMODITY), is(0.0));
    }
}
