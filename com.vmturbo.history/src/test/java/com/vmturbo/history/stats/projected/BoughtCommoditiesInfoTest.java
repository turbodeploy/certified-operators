package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_TYPE_WITH_KEY;
import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY_UNITS;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.stats.HistoryUtilizationType;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
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

    private static final StatValue TWO_VALUE_STAT = new StatsAccumulator()
            .record(5)
            .record(5)
            .toStatValue();

    private final IDataPack<Long> oidPack = new LongDataPack();
    private final IDataPack<String> keyPack = new DataPack<>();

    @Test
    public void testBoughtCommodityEmpty() {
        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .build(Mockito.mock(SoldCommoditiesInfo.class));
        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.emptySet(), Collections.emptySet()).isPresent());
    }

    @Test
    public void testBoughtCommodityEntityNotFound() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.singleton(1384L), Collections.emptySet()).isPresent());
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
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        final BoughtCommoditiesInfo info = BoughtCommoditiesInfo.newBuilder(
                Collections.emptySet(), keyPack, oidPack)
                .addEntity(vm)
                .build(soldCommoditiesInfo);

        StatValue usageStat = StatValue.newBuilder().setAvg(2).setMax(4).setMin(1).setTotal(4).setTotalMax(6).setTotalMin(4).build();
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
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(2).setMax(4).setMin(1).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(4).setMin(1).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder().setUsage(usageStat)
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setCapacity(TWO_VALUE_STAT).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vm.getOid()), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("expected record"));
        assertEquals(expectedStatRecord, record);
    }

    /**
     * Test registering exactly the same commodity - {type, key} - twice. The first is taken; the
     * second is ignored.
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
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        final BoughtCommoditiesInfo info = BoughtCommoditiesInfo.newBuilder(
                Collections.emptySet(), keyPack, oidPack)
                .addEntity(vm)
                .build(soldCommoditiesInfo);

        StatValue usageStat = StatValue.newBuilder().setAvg(1).setMax(2).setMin(1).setTotal(1).setTotalMax(2).setTotalMin(1).build();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(StatsAccumulator.singleStatValue((float)providerCapacity))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(1)
                .setProviderUuid(Long.toString(7))
                // Used and values are the same thing
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(1).setMax(2).setMin(1).setTotal(1).setTotalMax(2).setTotalMin(1).build())
                .setPeak(StatValue.newBuilder().setAvg(1).setMax(2).setMin(1).setTotal(1).setTotalMax(2).setTotalMin(1).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder().setUsage(usageStat)
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setCapacity(StatsAccumulator.singleStatValue((float)providerCapacity)).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vm.getOid()), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("expected record"));
        assertEquals(expectedStatRecord, record);
    }

    /**
     * Test registering exactly the same commodity - {type, key} - twice. The first is taken; the
     * second is ignored.
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

        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        final BoughtCommoditiesInfo boughtTwiceSameProvider = BoughtCommoditiesInfo.newBuilder(
                Collections.emptySet(), keyPack, oidPack)
                .addEntity(vmBuyingTwiceSameProvider)
                .build(soldCommoditiesInfo);

        final StatRecord record =
                boughtTwiceSameProvider.getAccumulatedRecord(COMMODITY,
                        Collections.singleton(vmBuyingTwiceSameProvider.getOid()), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("expected record"));

        StatValue usageStat = StatValue.newBuilder().setAvg(3).setMax(4).setMin(2).setTotal(6).setTotalMax(6).setTotalMin(6).build();
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
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(3).setMax(4).setMin(2).setTotal(6).setTotalMax(6).setTotalMin(6).build())
                .setPeak(StatValue.newBuilder().setAvg(3).setMax(4).setMin(2).setTotal(6).setTotalMax(6).setTotalMin(6).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(TWO_VALUE_STAT).build())
                .build();

        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testBoughtCommodityWholeMarket() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(8L)))
                .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        StatValue usageStat = StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(TWO_VALUE_STAT)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(4).setTotalMax(6).setTotalMin(4).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(TWO_VALUE_STAT).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.emptySet(), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("Expected record"));
        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testBoughtCommoditySingleEntity() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        final double providerCapacity = 5.0;
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(7L)))
                .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(VM_1.getOid()), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("Expected record"));

        StatValue usageStat = StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                // For now, capacity is the total capacity.
                .setCapacity(StatsAccumulator.singleStatValue((float)providerCapacity))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                .setProviderUuid(Long.toString(7))
                // Current value is the avg of used.
                .setCurrentValue(2)
                // Used and values are the same thing
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(StatsAccumulator.singleStatValue((float)providerCapacity)).build())
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
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_NO_PROVIDER)
                        .addEntity(VM_1)
                        .build(soldCommoditiesInfo);

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(VM_NO_PROVIDER.getOid()), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("Expected record"));

        StatValue usageStat = StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setCapacity(StatsAccumulator.singleStatValue(0))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                .setCurrentValue(2)
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .setPeak(StatValue.newBuilder().setAvg(2).setMax(3).setMin(2).setTotal(2).setTotalMax(3).setTotalMin(2).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(StatsAccumulator.singleStatValue(0)).build())
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
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(7L)))
                .thenReturn(Optional.of(providerCapacity));
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY), eq(4L)))
                .thenReturn(Optional.of(providerCapacity));

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_NO_PROVIDER)
                        .addEntity(VM_1)
                        .build(soldCommoditiesInfo);

        StatValue usageStat = StatValue.newBuilder().setAvg(4).setMax(9).setMin(2).setTotal(8).setTotalMax(12).setTotalMin(8).build();
        StatValue capacityStat = new StatsAccumulator().record(0).record(providerCapacity).toStatValue();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setCapacity(capacityStat)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                .setCurrentValue(4)
                // Used and values are the same thing
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(4).setMax(9).setMin(2).setTotal(8).setTotalMax(12).setTotalMin(8).build())
                .setPeak(StatValue.newBuilder().setAvg(4).setMax(9).setMin(2).setTotal(8).setTotalMax(12).setTotalMin(8).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(capacityStat).build())
                .build();

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.emptySet(), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("Expected record"));

        assertEquals(expectedStatRecord, record);
    }

    @Test
    public void testBoughtCommodityWholeMarketProviderNotFound() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        assertFalse(info.getAccumulatedRecord(COMMODITY, Collections.emptySet(), Collections.emptySet()).isPresent());
    }

    /**
     * Test that excluded commodities do not make it into the {@link BoughtCommoditiesInfo}.
     */
    @Test
    public void testBoughtCommodityExclusion() {
        final int commType = CommodityDTO.CommodityType.CLUSTER_VALUE;
        final String commodityName = HistoryStatsUtils.formatCommodityName(commType);

        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(1)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(7)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(commType))
                                .setUsed(2)
                                .setPeak(3)))
                .build();

        final BoughtCommoditiesInfo noExclusionInfo =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(vm)
                        .build(soldCommoditiesInfo);
        assertThat(noExclusionInfo.getValue(vm.getOid(), commodityName), is(2.0));

        final BoughtCommoditiesInfo info = BoughtCommoditiesInfo.newBuilder(
                Collections.singleton(CommodityDTO.CommodityType.CLUSTER), keyPack, oidPack)
                .addEntity(vm)
                .build(soldCommoditiesInfo);
        assertThat(info.getValue(vm.getOid(), commodityName), is(0.0));
    }

    @Test
    public void testBoughtCommoditySpecificEntityProviderNotFound() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .addEntity(VM_2)
                        .build(soldCommoditiesInfo);

        assertFalse(info.getAccumulatedRecord(COMMODITY,
                Collections.singleton(VM_1.getOid()), Collections.emptySet()).isPresent());
    }

    @Test
    public void testBoughtCommodityGetValue() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
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
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(vm)
                        .build(soldCommoditiesInfo);

        // Should be the average of the two commodities bought.
        assertThat(info.getValue(vm.getOid(), COMMODITY), is(5.0));
    }

    @Test
    public void testBoughtCommodityGetValueMissingCommodity() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .build(soldCommoditiesInfo);
        assertThat(info.getValue(VM_1.getOid(), "random commodity"), is(0.0));
    }

    @Test
    public void testBoughtCommodityGetValueMissingEntity() {
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(VM_1)
                        .build(soldCommoditiesInfo);
        assertThat(info.getValue(1234L, COMMODITY), is(0.0));
    }

    /**
     * Test get accumlated reocrd with selected commodity type and providerOid for an entity.
     */
    @Test
    public void testGetAccumulatedRecordWithProviderOid() {
        final long provider1Oid = 1001L;
        final double provider1used = 2001d;
        final double provider1peak = 3001d;
        final Optional<Double> provider1Capacity = Optional.of(5001d);
        final TopologyEntityDTO vmProvider1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(provider1Oid)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE_WITH_KEY)
                                .setUsed(provider1used)
                                .setPeak(provider1peak)
                        ))
                .build();

        final long provider2Oid = 1002L;
        final double provider2used = 2002d;
        final double provider2peak = 3002d;
        final Optional<Double> provider2Capacity = Optional.of(5002d);
        final TopologyEntityDTO vmProvider2 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(provider2Oid)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE_WITH_KEY)
                                .setUsed(provider2used)
                                .setPeak(provider2peak)
                        ))
                .build();
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY_TYPE_WITH_KEY.getKey()), eq(provider1Oid)))
                .thenReturn(provider1Capacity);
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY_TYPE_WITH_KEY.getKey()), eq(provider2Oid)))
                .thenReturn(provider2Capacity);

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(vmProvider1)
                        .addEntity(vmProvider2)
                        .build(soldCommoditiesInfo);

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vmProvider1.getOid()), Collections.singleton(provider1Oid))
                        .orElseThrow(() -> new RuntimeException("Expected record"));

        final float expectedUsed = (float)provider1used;
        final float expectedPeak = (float)provider1peak;
        final float expectedCapacity = provider1Capacity.get().floatValue();
        StatValue usageStat = StatValue.newBuilder().setAvg(expectedUsed).setMax(expectedPeak).setMin(expectedUsed)
                .setTotal(expectedUsed).setTotalMax(expectedPeak).setTotalMin(expectedUsed).build();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setProviderUuid(String.valueOf(provider1Oid))
                .setCapacity(StatsAccumulator.singleStatValue(expectedCapacity))
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                .setCurrentValue(expectedUsed)
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder().setAvg(expectedUsed).setMax(expectedPeak).setMin(expectedUsed).setTotal(expectedUsed).setTotalMax(expectedPeak).setTotalMin(expectedUsed).build())
                .setPeak(StatValue.newBuilder().setAvg(expectedUsed).setMax(expectedPeak).setMin(expectedUsed).setTotal(expectedUsed).setTotalMax(expectedPeak).setTotalMin(expectedUsed).build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(StatsAccumulator.singleStatValue(expectedCapacity)).build())
                .build();

        assertEquals(expectedStatRecord, record);
    }

    /**
     * Test get accumulated record with selected commodity type and providerOid for an entity.
     */
    @Test
    public void testGetAccumulatedRecordWithMultipleProviderButQueryWithNoProviderOid() {
        final long provider1Oid = 1001L;
        final double provider1used = 2001d;
        final double provider1peak = 3001d;
        final Optional<Double> provider1Capacity = Optional.of(5001d);
        final TopologyEntityDTO vmProvider1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(provider1Oid)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE_WITH_KEY)
                                .setUsed(provider1used)
                                .setPeak(provider1peak)
                        ))
                .build();

        final long provider2Oid = 1002L;
        final double provider2used = 2002d;
        final double provider2peak = 3002d;
        final Optional<Double> provider2Capacity = Optional.of(5002d);
        final TopologyEntityDTO vmProvider2 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setOid(2)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(provider2Oid)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(COMMODITY_TYPE_WITH_KEY)
                                .setUsed(provider2used)
                                .setPeak(provider2peak)
                        ))
                .build();
        final SoldCommoditiesInfo soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY_TYPE_WITH_KEY.getKey()), eq(provider1Oid)))
                .thenReturn(provider1Capacity);
        when(soldCommoditiesInfo.getCapacity(eq(COMMODITY_TYPE_WITH_KEY.getKey()), eq(provider2Oid)))
                .thenReturn(provider2Capacity);

        final BoughtCommoditiesInfo info =
                BoughtCommoditiesInfo.newBuilder(Collections.emptySet(), keyPack, oidPack)
                        .addEntity(vmProvider1)
                        .addEntity(vmProvider2)
                        .build(soldCommoditiesInfo);

        final StatRecord record =
                info.getAccumulatedRecord(COMMODITY, Collections.singleton(vmProvider1.getOid()), Collections.emptySet())
                        .orElseThrow(() -> new RuntimeException("Expected record"));


        final float expectedAverage = (float)(provider1used + provider2used) / 2;
        StatValue usageStat = StatValue.newBuilder()
                .setAvg(expectedAverage)
                .setMax((float)Math.max(provider1peak, provider2peak))
                .setMin((float)Math.min(provider1used, provider2used))
                .setTotal((float)(provider1used + provider2used))
                .setTotalMax((float)(provider1peak + provider2peak))
                .setTotalMin((float)(provider1used + provider2used))
                .build();
        StatValue capacityStat = StatValue.newBuilder()
                .setAvg((float)(provider1Capacity.get() + provider2Capacity.get()) / 2)
                .setMax((float)Math.max(provider1Capacity.get(), provider2Capacity.get()))
                .setMin((float)Math.min(provider1Capacity.get(), provider2Capacity.get()))
                .setTotal((float)(provider1Capacity.get() + provider2Capacity.get()))
                .setTotalMax((float)(provider1Capacity.get() + provider2Capacity.get()))
                .setTotalMin((float)(provider1Capacity.get() + provider2Capacity.get()))
                .build();
        final StatRecord expectedStatRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .setCapacity(capacityStat)
                .setUnits(COMMODITY_UNITS)
                .setRelation(RelationType.COMMODITIESBOUGHT.getLiteral())
                .setCurrentValue(expectedAverage)
                .setUsed(usageStat)
                .setValues(StatValue.newBuilder()
                        .setAvg(expectedAverage)
                        .setMax((float)Math.max(provider1peak, provider2peak))
                        .setMin((float)Math.min(provider1used, provider2used))
                        .setTotal((float)(provider1used + provider2used))
                        .setTotalMax((float)(provider1peak + provider2peak))
                        .setTotalMin((float)(provider1used + provider2used))
                        .build())
                .setPeak(StatValue.newBuilder()
                        .setAvg(expectedAverage)
                        .setMax((float)Math.max(provider1peak, provider2peak))
                        .setMin((float)Math.min(provider1used, provider2used))
                        .setTotal((float)(provider1used + provider2used))
                        .setTotalMax((float)(provider1peak + provider2peak))
                        .setTotalMin((float)(provider1used + provider2used))
                        .build())
                .addHistUtilizationValue(StatRecord.HistUtilizationValue.newBuilder()
                        .setType(HistoryUtilizationType.Smoothed.getApiParameterName())
                        .setUsage(usageStat).setCapacity(capacityStat)
                        .build()).build();
        assertEquals(expectedStatRecord, record);
    }

    /**
     * Should not throw an index out of bounds exception when the oid is not found in the oid pack.
     * Before the bug was fixed, this test would have failed due to a ArrayIndexOutOfBoundsException.
     */
    @Test
    public void testNoIndexOutOfBounds() {
        DataPack<Long> oidDataPack = new DataPack<>();
        DataPack<String> keyDataPack = new DataPack<>();
        SoldCommoditiesInfo soldCommoditiesInfo = SoldCommoditiesInfo
            .newBuilder(Collections.emptySet(), oidDataPack, keyDataPack)
            .build();
        DataPack<String> commodityNameDataPack = new DataPack<>();
        BoughtCommoditiesInfo boughtCommoditiesInfo = BoughtCommoditiesInfo
            .newBuilder(Collections.emptySet(), commodityNameDataPack, oidDataPack)
            .addEntity(VM_2)
            .build(soldCommoditiesInfo);

        oidDataPack.freeze(false);
        keyDataPack.freeze(false);
        commodityNameDataPack.freeze(false);

        boughtCommoditiesInfo.getAccumulatedRecord(
            "Mem",
            ImmutableSet.of(-1L),
            ImmutableSet.of(-1L));
    }
}
