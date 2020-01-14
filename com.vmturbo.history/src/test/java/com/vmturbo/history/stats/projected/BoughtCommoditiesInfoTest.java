package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE_WITH_KEY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_UNITS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.StatsAccumulator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class BoughtCommoditiesInfoTest {

    private static final TopologyEntityDTO VM_1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOid(1)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(7)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(COMMODITY_TYPE)
                    .setUsed(2)
                    .setPeak(3)))
            .build();

    private static final TopologyEntityDTO VM_2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOid(2)
            // Buying from a different provider than VM_1
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(8)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(COMMODITY_TYPE)
                    .setUsed(2)
                    .setPeak(3)))
            .build();

    public static final StatValue TWO_VALUE_STAT = new StatsAccumulator()
        .record(5)
        .record(5)
        .toStatValue();

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

    /**
     * Test registering two commodities with the same type but different keys. Both are saved.
     */
    @Test
    public void testBoughtCommodityDifferentKeys() {
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(7)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE)
                        .setUsed(1)
                        .setPeak(2))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE_WITH_KEY)
                        .setUsed(3)
                        .setPeak(4)))
                .build();

        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        final BoughtCommoditiesInfo info = BoughtCommoditiesInfo.newBuilder()
                .addEntity(vm)
                .build(soldCommoditiesInfo);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used - 1 & 3
                .setCurrentValue(2)
                .setProviderUuid(Long.toString(7))
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(4).setMin(1).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(4).setMin(1).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(4).setMin(1).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vm.getOid()))
                        .orElseThrow(() -> new RuntimeException("expected record"));
        assertEquals(expectedStatRecord, record);

    }
    /**
     * Test registering exactly the same commodity - {type, key} - twice. The first is taken;
     * the second is ignored.
     */
    @Test
    public void testBoughtCommodityDuplicate() {
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(7)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE)
                                .setUsed(1)
                                .setPeak(2))
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE)
                                .setUsed(3)
                                .setPeak(4)))
                .build();

        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.eq(COMMODITY), Mockito.eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        final BoughtCommoditiesInfo info = BoughtCommoditiesInfo.newBuilder()
                .addEntity(vm)
                .build(soldCommoditiesInfo);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(StatsAccumulator.singleStatValue((float) providerCapacity))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(1)
                .setProviderUuid(Long.toString(7))
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(1).setMax(2).setMin(1).setTotal(1).setTotalMax(2).setTotalMin(1).build())
                .setValues(StatValue.newBuilder().setAvg(1).setMax(2).setMin(1).setTotal(1).setTotalMax(2).setTotalMin(1).build())
                .setPeak(StatValue.newBuilder().setAvg(1).setMax(2).setMin(1).setTotal(1).setTotalMax(2).setTotalMin(1).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vm.getOid()))
                        .orElseThrow(() -> new RuntimeException("expected record"));
        assertEquals(expectedStatRecord, record);
    }

    /**
     * Test registering exactly the same commodity - {type, key} - twice. The first is taken;
     * the second is ignored.
     */
    @Test
    public void testBuyingTwiceSameProvider() {

        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;

        CommoditiesBoughtFromProvider commoditiesBoughtFromProvider1 = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(7)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE)
                        .setUsed(2)
                        .setPeak(2))
                .build();

        CommoditiesBoughtFromProvider commoditiesBoughtFromProvider2 = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(7)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(COMMODITY_TYPE)
                        .setUsed(4)
                        .setPeak(4))
                .build();

        final TopologyEntityDTO vmBuyingTwiceSameProvider = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(2)
                .addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider1)
                .addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider2)
                .build();

        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        final BoughtCommoditiesInfo boughtTwiceSameProvider = BoughtCommoditiesInfo.newBuilder()
                .addEntity(vmBuyingTwiceSameProvider)
                .build(soldCommoditiesInfo);

        final StatRecord record =
                boughtTwiceSameProvider.getAccumulatedRecord(COMMODITY, Collections.singleton(vmBuyingTwiceSameProvider.getOid()))
                        .orElseThrow(() -> new RuntimeException("expected record"));

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(3)
                .setProviderUuid(Long.toString(7))
                // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(3).setMax(4).setMin(2).setTotal(6).setTotalMax(6).setTotalMin(6).build())
                .setValues(StatValue.newBuilder().setAvg(3).setMax(4).setMin(2).setTotal(6).setTotalMax(6).setTotalMin(6).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(4).setMin(2).setTotal(6).setTotalMax(6).setTotalMin(6).build())
                .build();

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
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                    // Current value is the avg of used.
                .setCurrentValue(2)
                    // Used and values are the same thing
                .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("Expected record"));
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
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(VM_1.getOid()))
                        .orElseThrow(() -> new RuntimeException("Expected record"));

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
            .setName(COMMODITY)
            // For now, capacity is the total capacity.
            .setCapacity(StatsAccumulator.singleStatValue((float) providerCapacity))
            .setUnits(COMMODITY_UNITS)
            .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
            .setProviderUuid(Long.toString(7))
                // Current value is the avg of used.
            .setCurrentValue(2)
                // Used and values are the same thing
            .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
            .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
            .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
            .build();

        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testBoughtCommodityWithoutProviderSingle() {
        final TopologyEntityDTO VM_NO_PROVIDER = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOid(2)
            // no provider id
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(COMMODITY_TYPE)
                    .setUsed(2)
                    .setPeak(3)))
            .build();
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);

        final BoughtCommoditiesInfo info =
            BoughtCommoditiesInfo.newBuilder()
                .addEntity(VM_NO_PROVIDER)
                .addEntity(VM_1)
                .build(soldCommoditiesInfo);

        final StatRecord record =
            info.getAccumulatedRecord(COMMODITY, Collections.singleton(VM_NO_PROVIDER.getOid()))
                    .orElseThrow(() -> new RuntimeException("Expected record"));

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
            .setName(COMMODITY)
            .setCapacity(StatsAccumulator.singleStatValue(0))
            .setUnits(COMMODITY_UNITS)
            .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
            .setCurrentValue(2)
            .setUsed(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
            .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
            .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
            .build();

        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testBoughtCommodityWithoutProviderWholeMarket() {
        final TopologyEntityDTO VM_NO_PROVIDER = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOid(4)
            // no provider id
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(COMMODITY_TYPE)
                    .setUsed(6)
                    .setPeak(9)))
            .build();

        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(7L)))
            .thenReturn(Optional.of(providerCapacity));
        Mockito.when(soldCommoditiesInfo.getCapacity( Mockito.eq(COMMODITY), Mockito.eq(4L)))
            .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
            BoughtCommoditiesInfo.newBuilder()
                .addEntity(VM_NO_PROVIDER)
                .addEntity(VM_1)
                .build(soldCommoditiesInfo);

        final StatRecord expectedStatRecord = StatRecord.newBuilder()
            .setName(COMMODITY)
            .setCapacity(new StatsAccumulator().record(0).record(providerCapacity).toStatValue())
            .setUnits(COMMODITY_UNITS)
            .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
            .setCurrentValue(4)
            // Used and values are the same thing
            .setUsed(StatValue.newBuilder().setAvg(4).setMax(9).setMin(2).setTotal(8).setTotalMax(12).setTotalMin(8).build())
            .setValues(StatValue.newBuilder().setAvg(4).setMax(9).setMin(2).setTotal(8).setTotalMax(12).setTotalMin(8).build())
            .setPeak(StatValue.newBuilder().setAvg(4).setMax(9).setMin(2).setTotal(8).setTotalMax(12).setTotalMin(8).build())
            .build();

        final StatRecord record =
            info.getAccumulatedRecord(COMMODITY, Collections.emptySet())
                    .orElseThrow(() -> new RuntimeException("Expected record"));

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

    @Test
    public void testBoughtCommodityGetValue() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(7)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE)
                                .setUsed(2)))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(8)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE)
                                .setUsed(8)))
                .build();

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(vm)
                        .build(soldCommoditiesInfo);

        // Should be the average of the two commodities bought.
        assertThat(info.getValue(vm.getOid(), COMMODITY), is(5.0));
    }

    @Test
    public void testBoughtCommodityGetValueMissingCommodity() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .build(soldCommoditiesInfo);
        assertThat(info.getValue(VM_1.getOid(), "random commodity"), is(0.0));
    }

    @Test
    public void testBoughtCommodityGetValueMissingEntity() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder()
                        .addEntity(VM_1)
                        .build(soldCommoditiesInfo);
        assertThat(info.getValue(1234L, COMMODITY), is(0.0));
    }
}
