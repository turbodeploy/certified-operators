package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_UNITS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommodityBoughtList;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reports.db.RelationType;

public class BoughtCommoditiesInfoTest {

    private static final TopologyEntityDTO VM_1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOid(1)
            .putCommodityBoughtMap(7, CommodityBoughtList.newBuilder()
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(COMMODITY_TYPE)
                            .setUsed(2)
                            .setPeak(3))
                    .build())
            .build();

    private static final TopologyEntityDTO VM_2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOid(2)
            // Buying from a different provider than VM_1
            .putCommodityBoughtMap(8, CommodityBoughtList.newBuilder()
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(COMMODITY_TYPE)
                            .setUsed(2)
                            .setPeak(3))
                    .build())
            .build();

    @Test
    public void testBoughtCommodityEmpty() {
        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder().build(Mockito.mock(SoldCommoditiesInfo.class));
        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.emptySet()).isPresent());
    }

    @Test
    public void testBoughtCommodityEntityNotFound() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.singleton(1384L)).isPresent());
    }

    @Test
    public void testBoughtCommodityDuplicate() {
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(1)
                .putCommodityBoughtMap(7, CommodityBoughtList.newBuilder()
                        // Buying the same commodity twice, with different values.
                        // Expect the LATER value.
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE)
                                .setUsed(1)
                                .setPeak(2))
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE)
                                .setUsed(3)
                                .setPeak(4))
                        .build())
                .build();

        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(7L)))
                .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(vm)
                        .build(soldCommoditiesInfo);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity((float)providerCapacity)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(3)
                .setProviderUuid(Long.toString(7))
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(3).build())
                .setValues(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(3).build())
                .setPeak(StatValue.newBuilder().setAvg(4).setMax(4).setMin(4).setTotal(4).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vm.getOid())).get();
        assertEquals(expectedStatRecord, record);

    }

    @Test
    public void testBoughtCommodityWholeMarket() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(8L)))
                .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity((float)providerCapacity * 2)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(4).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(6).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.emptySet()).get();
        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testBoughtCommoditySingleEntity() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(7L)))
                .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(VM_1.getOid())).get();

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity((float)providerCapacity)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                .setProviderUuid(Long.toString(7))
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(2).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(2).setMin(2).setTotal(2).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(3).setMin(3).setTotal(3).build())
                .build();

        assertEquals(expectedStatRecord, record);

    }

    @Test
    public void testBoughtCommodityWholeMarketProviderNotFound() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.emptySet()).isPresent());
    }

    @Test
    public void testBoughtCommoditySpecificEntityProviderNotFound() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        assertFalse(info.getAccumulatedRecord(COMMODITY,
                Collections.singleton(VM_1.getOid())).isPresent());
    }

}
