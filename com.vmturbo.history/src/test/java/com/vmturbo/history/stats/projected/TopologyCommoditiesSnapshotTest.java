package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.history.stats.StatsTestUtils;
import com.vmturbo.history.stats.projected.ProjectedPriceIndexSnapshot.PriceIndexSnapshotFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

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
    private ProjectedPriceIndexSnapshot projectedPriceIndexSnapshot;

    private static final Set<String> EXCLUDED_COMMODITY_NAMES = Collections.singleton("CLUSTERCommodity");

    @Before
    public void setup() {
        soldCommoditiesInfo = mock(SoldCommoditiesInfo.class);
        when(soldCommoditiesInfo.getAccumulatedRecords(any(), any()))
               .thenReturn(Optional.empty());
        boughtCommoditiesInfo = mock(BoughtCommoditiesInfo.class);
        when(boughtCommoditiesInfo.getAccumulatedRecord(any(), any()))
                .thenReturn(Optional.empty());
        entityCountInfo = mock(EntityCountInfo.class);
        when(entityCountInfo.getCountRecord(any()))
                .thenReturn(Optional.empty());
        when(entityCountInfo.isCountStat(any())).thenReturn(false);
        projectedPriceIndexSnapshot = mock(ProjectedPriceIndexSnapshot.class);
    }

    @Test
    public void testCreateSnapshot() throws InterruptedException, TimeoutException, CommunicationException {
        RemoteIterator<ProjectedTopologyEntity> entities = mock(RemoteIterator.class);
        when(entities.hasNext()).thenReturn(true).thenReturn(false);
        final double priceIndex = 7.0;
        final ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(10)
                .setDisplayName("foo")
                .setOid(77L)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.MEM_VALUE))
                        .setUsed(10)))
            .setProjectedPriceIndex(priceIndex)
            .build();
        final Long2DoubleMap expectedPriceIndexMap = new Long2DoubleOpenHashMap();
        expectedPriceIndexMap.put(entity.getEntity().getOid(), priceIndex);

        when(entities.nextChunk()).thenReturn(Collections.singletonList(entity));
        final PriceIndexSnapshotFactory priceIndexSnapshotFactory = mock(PriceIndexSnapshotFactory.class);
        when(priceIndexSnapshotFactory.createSnapshot(expectedPriceIndexMap)).thenReturn(mock(ProjectedPriceIndexSnapshot.class));
        final TopologyCommoditiesSnapshot snapshot =
                TopologyCommoditiesSnapshot.newFactory(EXCLUDED_COMMODITY_NAMES).createSnapshot(entities, priceIndexSnapshotFactory);

        verify(priceIndexSnapshotFactory).createSnapshot(expectedPriceIndexMap);

        assertThat(snapshot.getTopologySize(), is(1L));
        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"), Collections.singleton(77L))
                        .collect(Collectors.toList());
        assertThat(records.size(), is(1));
        assertThat(records.get(0).getCurrentValue(), is(10.0f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoCommodities() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        snapshot.getRecords(Collections.emptySet(), Collections.emptySet());
    }

    @Test
    public void testEntityCountCommodities() {
        final String statName = "count1";

        when(entityCountInfo.isCountStat(statName)).thenReturn(true);
        when(entityCountInfo.getCountRecord(Mockito.eq(statName)))
               .thenReturn(Optional.of(DUMMY_1));

        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

         List<StatRecord> records = snapshot.getRecords(Collections.singleton("count1"),
                     Collections.emptySet())
                 .collect(Collectors.toList());
         assertEquals(1, records.size());
         assertEquals(DUMMY_1, records.get(0));
    }

    @Test
    public void testEntityCountCommoditiesEmpty() {
        final String statName = "count1";

        when(entityCountInfo.isCountStat(statName)).thenReturn(true);
        when(entityCountInfo.getCountRecord(Mockito.eq(statName)))
               .thenReturn(Optional.empty());

        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        List<StatRecord> records = snapshot.getRecords(Collections.singleton("count1"),
                Collections.emptySet())
                .collect(Collectors.toList());
        assertEquals(0, records.size());
    }

    @Test
    public void testTopologySize() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);
        assertEquals(1, snapshot.getTopologySize());
    }

    @Test
    public void testPriceIndexFallthroughToPriceIndexSnapshot() {
        final Set<Long> entities = Collections.singleton(1L);
        final StatRecord statRecord = StatRecord.newBuilder()
                .setName("foo")
                .build();
        when(projectedPriceIndexSnapshot.getRecord(entities)).thenReturn(Optional.of(statRecord));
        final TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);
        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton(StringConstants.PRICE_INDEX), entities)
                .collect(Collectors.toList());
        verify(projectedPriceIndexSnapshot).getRecord(entities);
        assertThat(records, contains(statRecord));
    }

    @Test
    public void testCommodityNotSoldOrBought() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"),
                        Collections.emptySet())
                        .collect(Collectors.toList());

        verify(soldCommoditiesInfo).getAccumulatedRecords(
                Mockito.eq("Mem"), Mockito.eq(Collections.emptySet()));
        verify(boughtCommoditiesInfo).getAccumulatedRecord(
                Mockito.eq("Mem"), Mockito.eq(Collections.emptySet()));

        assertEquals(0, records.size());
    }

    @Test
    public void testCommoditySoldOnly() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        when(soldCommoditiesInfo.getAccumulatedRecords(
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
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        when(boughtCommoditiesInfo.getAccumulatedRecord(
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
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        when(soldCommoditiesInfo.getAccumulatedRecords(
                Mockito.eq(COMMODITY), Mockito.eq(Collections.emptySet())))
                .thenReturn(Optional.of(DUMMY_1));
        when(boughtCommoditiesInfo.getAccumulatedRecord(
                Mockito.eq(COMMODITY), Mockito.eq(Collections.emptySet())))
                .thenReturn(Optional.of(DUMMY_2));

        final List<StatRecord> records =
                snapshot.getRecords(Collections.singleton("Mem"),
                        Collections.emptySet())
                        .collect(Collectors.toList());
        assertEquals(2, records.size());
        assertThat(records, containsInAnyOrder(DUMMY_1, DUMMY_2));
    }

    @Test
    public void testEntityComparatorAscending() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.isAscending()).thenReturn(true);

        final long smallerId = 7L;
        final double smallerVal = 10.0;
        final long largerId = 77L;
        final double largerVal = 20.0;
        when(soldCommoditiesInfo.getValue(smallerId, COMMODITY)).thenReturn(smallerVal);
        when(soldCommoditiesInfo.getValue(largerId, COMMODITY)).thenReturn(largerVal);

        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params,
            StatsTestUtils.createEntityGroupsMap(Sets.newHashSet(smallerId, largerId)));
        final int result = entityComparator.compare(smallerId, largerId);
        assertThat(result, is(-1));
    }

    @Test
    public void testEntityComparatorDescending() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.isAscending()).thenReturn(false);

        final long smallerId = 7L;
        final double smallerVal = 10.0;
        final long largerId = 77L;
        final double largerVal = 20.0;
        when(soldCommoditiesInfo.getValue(smallerId, COMMODITY)).thenReturn(smallerVal);
        when(soldCommoditiesInfo.getValue(largerId, COMMODITY)).thenReturn(largerVal);

        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params,
            StatsTestUtils.createEntityGroupsMap(Sets.newHashSet(smallerId, largerId)));
        final int result = entityComparator.compare(smallerId, largerId);
        assertThat(result, is(1));
    }

    @Test
    public void testEntityComparatorEqualStatValueAscending() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.isAscending()).thenReturn(true);

        final long smallerId = 7L;
        final long largerId = 8L;
        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params,
            StatsTestUtils.createEntityGroupsMap(Sets.newHashSet(smallerId, largerId)));
        // The stat values are the same, so the order should be determined by the id.
        final int result = entityComparator.compare(smallerId, largerId);
        assertThat(result, is(-1));
    }

    @Test
    public void testEntityComparatorEqualStatValueDescending() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.isAscending()).thenReturn(false);

        final long smallerId = 7L;
        final long largerId = 8L;
        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params,
            StatsTestUtils.createEntityGroupsMap(Sets.newHashSet(smallerId, largerId)));
        // The stat values are the same, so the order should be determined by the id.
        final int result = entityComparator.compare(smallerId, largerId);
        assertThat(result, is(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEntityComparatorCountStat() {
        TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(entityCountInfo.isCountStat(COMMODITY)).thenReturn(true);
        snapshot.getEntityComparator(params, Collections.emptyMap());
    }

    @Test
    public void testEntityComparatorPriceIndex() {
        final TopologyCommoditiesSnapshot snapshot =
                new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                        entityCountInfo, projectedPriceIndexSnapshot, 1);

        final Comparator<Long> priceIndexComparator = mock(Comparator.class);
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(projectedPriceIndexSnapshot.getEntityComparator(params)).thenReturn(priceIndexComparator);
        when(params.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        assertThat(snapshot.getEntityComparator(params, Collections.emptyMap()), is(priceIndexComparator));
    }

    /**
     * Test the comparator created for EntityGroup works as expected. The stats value is aggregated
     * from derived entities, then used for comparison.
     */
    @Test
    public void testEntityComparatorEntityGroupAscending() {
        final TopologyCommoditiesSnapshot snapshot =
            new TopologyCommoditiesSnapshot(soldCommoditiesInfo, boughtCommoditiesInfo,
                entityCountInfo, projectedPriceIndexSnapshot, 1);
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.isAscending()).thenReturn(true);

        final long dcId1 = 7L;
        final long dcId2 = 8L;
        final long pmId1 = 71L;
        final long pmId2 = 72L;
        final long pmId3 = 81L;
        final long pmId4 = 82L;
        final double pmVal1 = 11.0;
        final double pmVal2 = 12.0;
        final double pmVal3 = 13.0;
        final double pmVal4 = 15.0;

        final Map<Long, Double> valuesMap = ImmutableMap.of(
            pmId1, pmVal1,
            pmId2, pmVal2,
            pmId3, pmVal3,
            pmId4, pmVal4
        );
        valuesMap.forEach((key, value) ->
            when(soldCommoditiesInfo.getValue(key, COMMODITY)).thenReturn(value));

        final Comparator<Long> entityComparator1 = snapshot.getEntityComparator(params,
            ImmutableMap.of(
                dcId1, ImmutableSet.of(pmId1, pmId2),
                dcId2, ImmutableSet.of(pmId3, pmId4)
            ));
        assertThat(entityComparator1.compare(dcId1, dcId2), is(-1));

        final Comparator<Long> entityComparator2 = snapshot.getEntityComparator(params,
            ImmutableMap.of(
                dcId1, ImmutableSet.of(pmId1, pmId4),
                dcId2, ImmutableSet.of(pmId2, pmId3)
            ));
        assertThat(entityComparator2.compare(dcId1, dcId2), is(1));
    }
}
