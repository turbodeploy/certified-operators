package com.vmturbo.history.stats.projected;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.HistoryUtilizationType;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedBoughtCommodity;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedSoldCommodity;
import com.vmturbo.history.stats.projected.BoughtCommoditiesInfo.BoughtCommodity;
import com.vmturbo.history.stats.projected.SoldCommoditiesInfo.SoldCommodity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

public class AccumulatedCommodityTest {

    private final static String COMMODITY = "Mem";

    private final static CommodityType MEM_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.MEM.getNumber())
            .build();

    private final static String COMMODITY_UNITS = "KB";

    public static final StatValue TWO_VALUE_STAT = new StatsAccumulator()
        .record(5)
        .record(5)
        .toStatValue();
    private static final StatValue PERCENTILE_USAGE = StatValue.newBuilder()
    .setAvg(2.5F)
    .setMax(2.5F)
    .setMin(2.5F)
    .setTotal(5F)
    .setTotalMax(5F)
    .setTotalMin(5F).build();

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
        SoldCommodity soldCommodity = new SoldCommodity(CommoditySoldDTO.newBuilder()
                .setCommodityType(MEM_COMMODITY_TYPE)
                .setUsed(3)
                .setPeak(4)
                .setCapacity(5)
                .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(0.5D).build())
                .build());
        // Add two of the same commodity (to make the math easier)
        commodity.recordSoldCommodity(soldCommodity);
        commodity.recordSoldCommodity(soldCommodity);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIES.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(3)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
                .setValues(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
                .addHistUtilizationValue(HistUtilizationValue.newBuilder()
                                .setType(HistoryUtilizationType.Percentile
                                                .getApiParameterName())
                                .setUsage(PERCENTILE_USAGE)
                                .setCapacity(TWO_VALUE_STAT))
                                .build();


        assertEquals(expectedStatRecord, commodity.toStatRecord().get());
    }

    @Test
    public void testAccumulatedBoughtCommodity() {
        final AccumulatedBoughtCommodity commodity =
                new AccumulatedBoughtCommodity(COMMODITY);

        final BoughtCommodity boughtComm = new BoughtCommodity(CommodityBoughtDTO.newBuilder()
                .setCommodityType(MEM_COMMODITY_TYPE)
                .setUsed(3)
                .setPeak(4)
                .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(0.5D).build())
                .build());

        commodity.recordBoughtCommodity(boughtComm, 1L, 5);
        commodity.recordBoughtCommodity(boughtComm, 2L, 5);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(3)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
                .setValues(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
                .addHistUtilizationValue(HistUtilizationValue.newBuilder()
                                .setType(HistoryUtilizationType.Percentile
                                                .getApiParameterName())
                                .setUsage(PERCENTILE_USAGE)
                                .setCapacity(TWO_VALUE_STAT)).build();

        assertEquals(expectedStatRecord, commodity.toStatRecord().get());
    }

    @Test
    public void testAccumulatedBoughtCommodityWithoutProvider() {
        final AccumulatedBoughtCommodity commodity =
            new AccumulatedBoughtCommodity(COMMODITY);

        final BoughtCommodity boughtComm = new BoughtCommodity(CommodityBoughtDTO.newBuilder()
            .setCommodityType(MEM_COMMODITY_TYPE)
            .setUsed(3)
            .setPeak(4)
            .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(0.5D).build())
            .build());

        commodity.recordBoughtCommodity(boughtComm, TopologyCommoditiesSnapshot.NO_PROVIDER_ID, 0);
        commodity.recordBoughtCommodity(boughtComm, TopologyCommoditiesSnapshot.NO_PROVIDER_ID, 0);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
            .setName(COMMODITY)
            .setCapacity(StatsAccumulator.singleStatValue(0))
            .setUnits(COMMODITY_UNITS)
            .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
            .setCurrentValue(3)
            .setUsed(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
            .setValues(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
            .setPeak(StatValue.newBuilder().setAvg(3).setMax(4).setMin(3).setTotal(6).setTotalMax(8).setTotalMin(6).build())
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

        final BoughtCommodity boughtComm = new BoughtCommodity(CommodityBoughtDTO.newBuilder()
                .setCommodityType(MEM_COMMODITY_TYPE)
                .setUsed(3)
                .setPeak(4)
                .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(0.5D).build())
                .build());

        commodity.recordBoughtCommodity(boughtComm, 1L, 5);
        commodity.recordBoughtCommodity(boughtComm, 1L, 5);

        final StatRecord record = commodity.toStatRecord().get();
        assertEquals(Long.toString(1), record.getProviderUuid());
    }
}
