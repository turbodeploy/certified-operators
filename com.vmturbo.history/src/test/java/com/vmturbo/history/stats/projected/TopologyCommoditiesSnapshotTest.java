package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;

public class TopologyCommoditiesSnapshotTest {

    private static final StatRecord DUMMY_1 = StatRecord.newBuilder()
            .setName("DUMB")
            .build();

    private static final StatRecord DUMMY_2 = StatRecord.newBuilder()
            .setName("DUMBER")
            .build();

    private SoldCommoditiesInfo soldCommoditiesInfo;
    private BoughtCommoditiesInfo boughtCommoditiesInfo;
    private EntityCountInfo entityCountInfo;

    @Before
    public void setup() {
        soldCommoditiesInfo = Mockito.mock(SoldCommoditiesInfo.class);
        Mockito.when(soldCommoditiesInfo.getAccumulatedRecords(Mockito.any(), Mockito.any()))
               .thenReturn(Optional.empty());
        boughtCommoditiesInfo = Mockito.mock(BoughtCommoditiesInfo.class);
        Mockito.when(boughtCommoditiesInfo.getAccumulatedRecord(Mockito.any(), Mockito.any()))
                .thenReturn(Optional.empty());
        entityCountInfo = Mockito.mock(EntityCountInfo.class);
        Mockito.when(entityCountInfo.getCountRecord(Mockito.any()))
                .thenReturn(Optional.empty());
        Mockito.when(entityCountInfo.isCountStat(Mockito.any())).thenReturn(false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoCommodities() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

        snapshot.getRecords(Collections.emptySet(), Collections.emptySet());
    }

    @Test
    public void testEntityCountCommodities() {
        final String statName = "count1";

        Mockito.when(entityCountInfo.isCountStat(statName)).thenReturn(true);
        Mockito.when(entityCountInfo.getCountRecord(Mockito.eq(statName)))
               .thenReturn(Optional.of(DUMMY_1));

        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

         List<StatRecord> records = snapshot.getRecords(Collections.singleton("count1"),
                     Collections.emptySet())
                 .collect(Collectors.toList());
         assertEquals(1, records.size());
         assertEquals(DUMMY_1, records.get(0));
    }

    @Test
    public void testEntityCountCommoditiesEmpty() {
        final String statName = "count1";

        Mockito.when(entityCountInfo.isCountStat(statName)).thenReturn(true);
        Mockito.when(entityCountInfo.getCountRecord(Mockito.eq(statName)))
               .thenReturn(Optional.empty());

        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

        List<StatRecord> records = snapshot.getRecords(Collections.singleton("count1"),
                Collections.emptySet())
                .collect(Collectors.toList());
        assertEquals(0, records.size());
    }

    @Test
    public void testTopologySize() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);
        assertEquals(1, snapshot.getTopologySize());
    }

    @Test
    public void testPriceIndexIgnored() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);
        List<StatRecord> records =
                snapshot.getRecords(Collections.singleton(ProjectedStatsStore.PRICE_INDEX_NAME),
                        Collections.emptySet())
                .collect(Collectors.toList());
        assertEquals(0, records.size());
    }

    @Test
    public void testCommodityNotSoldOrBought() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"),
                        Collections.emptySet())
                        .collect(Collectors.toList());

        Mockito.verify(soldCommoditiesInfo).getAccumulatedRecords(
                Mockito.eq("Mem"), Mockito.eq(Collections.emptySet()));
        Mockito.verify(boughtCommoditiesInfo).getAccumulatedRecord(
                Mockito.eq("Mem"), Mockito.eq(Collections.emptySet()));

        assertEquals(0, records.size());
    }

    @Test
    public void testCommoditySoldOnly() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

        Mockito.when(soldCommoditiesInfo.getAccumulatedRecords(
                    Mockito.eq(COMMODITY), Mockito.eq(Collections.emptySet())))
               .thenReturn(Optional.of(DUMMY_1));

        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"),
                        Collections.emptySet())
                        .collect(Collectors.toList());
        assertEquals(1, records.size());
        assertEquals(DUMMY_1, records.get(0));
    }

    @Test
    public void testCommodityBoughtOnly() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

        Mockito.when(boughtCommoditiesInfo.getAccumulatedRecord(
                Mockito.eq(COMMODITY), Mockito.eq(Collections.emptySet())))
                .thenReturn(Optional.of(DUMMY_1));

        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"),
                        Collections.emptySet())
                        .collect(Collectors.toList());
        assertEquals(1, records.size());
        assertEquals(DUMMY_1, records.get(0));
    }

    @Test
    public void testCommoditySoldAndBought() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, 1);

        Mockito.when(soldCommoditiesInfo.getAccumulatedRecords(
                Mockito.eq(COMMODITY), Mockito.eq(Collections.emptySet())))
                .thenReturn(Optional.of(DUMMY_1));
        Mockito.when(boughtCommoditiesInfo.getAccumulatedRecord(
                Mockito.eq(COMMODITY), Mockito.eq(Collections.emptySet())))
                .thenReturn(Optional.of(DUMMY_2));

        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"),
                        Collections.emptySet())
                        .collect(Collectors.toList());
        assertEquals(2, records.size());
        assertThat(records, containsInAnyOrder(DUMMY_1, DUMMY_2));
    }
}
