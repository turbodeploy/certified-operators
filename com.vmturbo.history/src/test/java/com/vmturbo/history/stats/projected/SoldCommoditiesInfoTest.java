package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE;
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
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reports.db.RelationType;

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


    @Test
    public void testEmpty() {
        SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .build();

        assertFalse(info.getCapacity(COMMODITY, 1L).isPresent());
        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.emptySet()).isPresent());
    }

    @Test
    public void testSoldCommoditiesCapacity() {

        SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .build();

        assertEquals(Double.valueOf(4), info.getCapacity(COMMODITY, 1L).get());
    }

    @Test
    public void testSoldCommoditiesWholeMarket() {

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        final StatRecord record = info.getAccumulatedRecord(COMMODITY, Collections.emptySet()).get();

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(8)
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
                info.getAccumulatedRecord(COMMODITY, Sets.newHashSet(1L, 2L)).get();

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(8)
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
    public void testSoldCommoditiesDuplicate() {
        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE)
                        .setUsed(2)
                        .setPeak(3)
                        .setCapacity(4))
                // The same commodity spec, different values. Expect match on the
                // latest value.
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE)
                        .setUsed(5)
                        .setPeak(6)
                        .setCapacity(7))
                .build();

        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(pm)
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.emptySet()).get();

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(7)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(5)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(5).setMax(5).setMin(5).setTotal(5).build())
                .setValues(StatValue.newBuilder().setAvg(5).setMax(5).setMin(5).setTotal(5).build())
                .setPeak(StatValue.newBuilder().setAvg(6).setMax(6).setMin(6).setTotal(6).build())
                .build();

        assertEquals(expectedStatRecord, record);

    }

    @Test
    public void testSoldCommodityEntityNotFound() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        assertFalse(info.getAccumulatedRecord(COMMODITY, Sets.newHashSet(999L)).isPresent());
    }

    @Test
    public void testNotSoldCommodity() {
        final SoldCommoditiesInfo info = SoldCommoditiesInfo.newBuilder()
                .addEntity(PM_1)
                .addEntity(PM_2)
                .build();

        assertFalse(info.getAccumulatedRecord("beer", Sets.newHashSet(1L)).isPresent());
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

        assertFalse(info.getAccumulatedRecord("CPU", Sets.newHashSet(PM_1.getOid())).isPresent());
    }
}
