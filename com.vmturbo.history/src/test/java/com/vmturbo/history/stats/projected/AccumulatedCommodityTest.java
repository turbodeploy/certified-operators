package com.vmturbo.history.stats.projected;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.OptionalLong;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedSoldCommodity;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedBoughtCommodity;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.Accumulation;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.reports.db.RelationType;

public class AccumulatedCommodityTest {

    private final static String COMMODITY = "Mem";

    private final static CommodityType MEM_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.MEM.getNumber())
            .build();

    private final static String COMMODITY_UNITS = "KB";


    @Test
    public void testAccumulationOneValue() {
        final Accumulation value = new Accumulation();
        value.record(1);
        assertEquals(1, value.getMin(), 0);
        assertEquals(1, value.getMax(), 0);
        assertEquals(1, value.getAvg(), 0);
        assertEquals(1, value.getTotal(), 0);

        final StatValue statValue = value.toStatValue();
        assertEquals(1, statValue.getMin(), 0);
        assertEquals(1, statValue.getMax(), 0);
        assertEquals(1, statValue.getAvg(), 0);
        assertEquals(1, statValue.getTotal(), 0);
    }

    @Test
    public void testAccumulationTwoValues() {
        final Accumulation value = new Accumulation();
        value.record(1);
        value.record(3);
        assertEquals(1, value.getMin(), 0);
        assertEquals(3, value.getMax(), 0);
        assertEquals(2, value.getAvg(), 0);
        assertEquals(4, value.getTotal(), 0);

        final StatValue statValue = value.toStatValue();
        assertEquals(1, statValue.getMin(), 0);
        assertEquals(3, statValue.getMax(), 0);
        assertEquals(2, statValue.getAvg(), 0);
        assertEquals(4, statValue.getTotal(), 0);
    }

    @Test
    public void testAccumulatedSoldCommodityEmpty() {
        final AccumulatedSoldCommodity commodity =
                new AccumulatedSoldCommodity("commodity");
        assertFalse(commodity.toStatRecord().isPresent());
    }

    @Test
    public void testAccumulatedSoldCommodity() {
        final AccumulatedSoldCommodity commodity =
                new AccumulatedSoldCommodity(COMMODITY);
        CommoditySoldDTO soldCommodity = CommoditySoldDTO.newBuilder()
                .setCommodityType(MEM_COMMODITY_TYPE)
                .setUsed(3)
                .setPeak(4)
                .setCapacity(5)
                .build();
        // Add two of the same commodity (to make the math easier)
        commodity.recordSoldCommodity(soldCommodity);
        commodity.recordSoldCommodity(soldCommodity);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(10)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(3)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .setValues(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .setPeak(StatValue.newBuilder().setAvg(4).setMax(4).setMin(4).setTotal(8).build())
                .build();


        assertEquals(expectedStatRecord, commodity.toStatRecord().get());
    }

    @Test
    public void testAccumulatedBoughtCommodity() {
        final AccumulatedBoughtCommodity commodity =
                new AccumulatedBoughtCommodity(COMMODITY);

        final CommodityBoughtDTO dto = CommodityBoughtDTO.newBuilder()
                .setCommodityType(MEM_COMMODITY_TYPE)
                .setUsed(3)
                .setPeak(4)
                .build();

        commodity.recordBoughtCommodity(dto, 1L, 5);
        commodity.recordBoughtCommodity(dto, 2L, 5);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(10)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(3)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .setValues(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .setPeak(StatValue.newBuilder().setAvg(4).setMax(4).setMin(4).setTotal(8).build())
                .build();

        assertEquals(expectedStatRecord, commodity.toStatRecord().get());
    }

    @Test
    public void testAccumulatedBoughtCommodityWithoutProvider() {
        final AccumulatedBoughtCommodity commodity =
            new AccumulatedBoughtCommodity(COMMODITY);

        final CommodityBoughtDTO dto = CommodityBoughtDTO.newBuilder()
            .setCommodityType(MEM_COMMODITY_TYPE)
            .setUsed(3)
            .setPeak(4)
            .build();

        commodity.recordBoughtCommodity(dto, null, 0);
        commodity.recordBoughtCommodity(dto, null, 0);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
            .setName(COMMODITY)
            .setCapacity(0)
            .setUnits(COMMODITY_UNITS)
            .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
            .setCurrentValue(3)
            .setUsed(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
            .setValues(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
            .setPeak(StatValue.newBuilder().setAvg(4).setMax(4).setMin(4).setTotal(8).build())
            .build();

        assertEquals(expectedStatRecord, commodity.toStatRecord().get());
    }

    /**
     * Test that if an {@link AccumulatedBoughtCommodity} has commodities from only single provider,
     * the resulting {@link StatRecord} has the provider's ID.
     */
    @Test
    public void testBoughtCommoditySingleProvider() {
        final AccumulatedBoughtCommodity commodity =
                new AccumulatedBoughtCommodity(COMMODITY);

        final CommodityBoughtDTO dto = CommodityBoughtDTO.newBuilder()
                .setCommodityType(MEM_COMMODITY_TYPE)
                .setUsed(3)
                .setPeak(4)
                .build();

        commodity.recordBoughtCommodity(dto, 1L, 5);
        commodity.recordBoughtCommodity(dto, 1L, 5);

        final StatRecord record = commodity.toStatRecord().get();
        assertEquals(Long.toString(1), record.getProviderUuid());
    }
}
