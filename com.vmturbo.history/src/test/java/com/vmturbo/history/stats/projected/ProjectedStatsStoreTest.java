package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.stats.StatsTestUtils;
import com.vmturbo.history.stats.projected.ProjectedPriceIndexSnapshot.PriceIndexSnapshotFactory;
import com.vmturbo.history.stats.projected.ProjectedStatsStore.EntityStatsCalculator;
import com.vmturbo.history.stats.projected.ProjectedStatsStore.StatSnapshotCalculator;
import com.vmturbo.history.stats.projected.TopologyCommoditiesSnapshot.TopologyCommoditiesSnapshotFactory;

public class ProjectedStatsStoreTest {

    private TopologyCommoditiesSnapshotFactory snapshotFactory =
            mock(TopologyCommoditiesSnapshotFactory.class);

    private PriceIndexSnapshotFactory priceIndexSnapshotFactory =
            mock(PriceIndexSnapshotFactory.class);

    private StatSnapshotCalculator statSnapshotCalculator =
            mock(StatSnapshotCalculator.class);

    private EntityStatsCalculator entityStatsCalculator =
            mock(EntityStatsCalculator.class);

    private ProjectedStatsStore store = new ProjectedStatsStore(
            snapshotFactory, priceIndexSnapshotFactory, statSnapshotCalculator, entityStatsCalculator);

    @Captor
    private ArgumentCaptor<Collection<TopologyEntityDTO>> entitiesCaptor;

    @SuppressWarnings("unchecked")
    private RemoteIterator<ProjectedTopologyEntity> emptyIterator =
            (RemoteIterator<ProjectedTopologyEntity>)mock(RemoteIterator.class);



    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(emptyIterator.hasNext()).thenReturn(false);
    }

    @Test
    public void testDefaultConstructor() {
        new ProjectedStatsStore();
    }

    @Test
    public void testSnapshotCalculator() throws Exception {
        final StatSnapshotCalculator statSnapshotCalculator = new StatSnapshotCalculator() {};

        final Set<Long> entities = Sets.newHashSet(1L, 2L);
        final Set<String> commodities = Sets.newHashSet(COMMODITY, StringConstants.PRICE_INDEX);

        final StatRecord statRecord = StatRecord.newBuilder()
                .setName(COMMODITY)
                .build();

        final TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        when(snapshot.getRecords(commodities, entities)).thenReturn(Stream.of(statRecord));

        final StatSnapshot statSnapshot = statSnapshotCalculator.buildSnapshot(snapshot, entities, commodities);

        verify(snapshot).getRecords(commodities, entities);

        assertThat(statSnapshot.getStatRecordsList(), containsInAnyOrder(statRecord));
    }

    @Test
    public void testGetSnapshot() throws InterruptedException, TimeoutException, CommunicationException {
        final TopologyCommoditiesSnapshot topoSnapshot = mock(TopologyCommoditiesSnapshot.class);
        when(snapshotFactory.createSnapshot(emptyIterator, priceIndexSnapshotFactory))
                .thenReturn(topoSnapshot);
        store.updateProjectedTopology(emptyIterator);

        final Set<Long> entities = Sets.newHashSet(2L, 1L, 3L);
        final Set<String> commodities = Sets.newHashSet(COMMODITY);

        final StatSnapshot snapshot = StatSnapshot.newBuilder()
                .setSnapshotDate(1L)
                .build();
        when(statSnapshotCalculator.buildSnapshot(topoSnapshot, entities, commodities))
            .thenReturn(snapshot);
        final Optional<StatSnapshot> retSnapshot = store.getStatSnapshotForEntities(entities, commodities);
        assertThat(retSnapshot.get(), is(snapshot));
    }

    @Test
    public void testGetSnapshotNoData() {
        // Initially the store has no data.
        assertFalse(store.getStatSnapshotForEntities(Collections.emptySet(), Collections.emptySet()).isPresent());
    }

    @Test
    public void testUpdateSnapshot() throws Exception {
        final double priceIndex = 7.0;
        final ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
                .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(1)
                    .setOid(10))
                .setProjectedPriceIndex(priceIndex)
                .build();

        @SuppressWarnings("unchecked")
        final RemoteIterator<ProjectedTopologyEntity> remoteIterator =
                (RemoteIterator<ProjectedTopologyEntity>)Mockito.mock(RemoteIterator.class);

        when(remoteIterator.hasNext()).thenReturn(true, false);
        when(remoteIterator.nextChunk()).thenReturn(Collections.singletonList(entity));

        TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        when(snapshot.getTopologySize()).thenReturn(7L);

        when(snapshotFactory.createSnapshot(remoteIterator, priceIndexSnapshotFactory))
               .thenReturn(snapshot);

        assertEquals(7L, store.updateProjectedTopology(remoteIterator));

        verify(snapshotFactory).createSnapshot(remoteIterator, priceIndexSnapshotFactory);
    }

    @Test
    public void testGetEntityStats() throws Exception {
        final TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        when(snapshotFactory.createSnapshot(emptyIterator, priceIndexSnapshotFactory))
                .thenReturn(snapshot);
        store.updateProjectedTopology(emptyIterator);

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(COMMODITY);
        final Map<Long, Set<Long>> targetEntities = StatsTestUtils.createEntityGroupsMap(
            Collections.singleton(1L));
        final Set<String> targetCommodities = Collections.singleton("foo");

        final ProjectedEntityStatsResponse responseProto = ProjectedEntityStatsResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.getDefaultInstance())
                .build();
        when(entityStatsCalculator.calculateNextPage(snapshot, statSnapshotCalculator,
                targetEntities, targetCommodities, paginationParams))
            .thenReturn(responseProto);

        final ProjectedEntityStatsResponse response =
                store.getEntityStats(targetEntities, targetCommodities, paginationParams);

        verify(entityStatsCalculator).calculateNextPage(snapshot, statSnapshotCalculator,
                targetEntities, targetCommodities, paginationParams);

        assertThat(response, is(responseProto));
    }

    @Test
    public void testGetEntityStatsNoTopology() throws Exception {
        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(COMMODITY);
        final Map<Long, Set<Long>> targetEntities = StatsTestUtils.createEntityGroupsMap(
            Collections.singleton(1L));
        final Set<String> targetCommodities = Collections.singleton("foo");

        store.getEntityStats(targetEntities, targetCommodities, paginationParams);

        verifyZeroInteractions(entityStatsCalculator);
    }

    @Test
    public void testGetEntityStatsEmpty() {
        final ProjectedEntityStatsResponse response = store.getEntityStats(Collections.emptyMap(),
                Collections.emptySet(), mock(EntityStatsPaginationParams.class));
        assertThat(response, is(ProjectedEntityStatsResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.getDefaultInstance())
                .build()));
    }

    @Test
    public void testCalculateNextPageSortByCommodity() {
        final TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        final StatSnapshotCalculator snapshotCalculator = mock(StatSnapshotCalculator.class);

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(COMMODITY);
        when(paginationParams.getLimit()).thenReturn(2);
        when(paginationParams.getNextCursor()).thenReturn(Optional.empty());

        final Set<Long> entities = Sets.newHashSet(2L, 1L, 3L);
        final Set<String> commodities = Sets.newHashSet(COMMODITY);

        final StatSnapshot snapshot1 = StatSnapshot.newBuilder()
                .setSnapshotDate(1L)
                .build();
        final StatSnapshot snapshot2 = StatSnapshot.newBuilder()
                .setSnapshotDate(1L)
                .build();
        when(snapshot.getEntityComparator(paginationParams,
            StatsTestUtils.createEntityGroupsMap(entities))).thenReturn(Long::compare);
        when(snapshotCalculator.buildSnapshot(snapshot, Collections.singleton(1L), commodities))
                .thenReturn(snapshot1);
        when(snapshotCalculator.buildSnapshot(snapshot, Collections.singleton(2L), commodities))
                .thenReturn(snapshot2);

        final EntityStatsCalculator entityStatsCalculator = new EntityStatsCalculator() {};
        final ProjectedEntityStatsResponse response = entityStatsCalculator.calculateNextPage(
            snapshot, snapshotCalculator, StatsTestUtils.createEntityGroupsMap(entities),
            commodities, paginationParams);
        assertThat(response.getEntityStatsList(), contains(
                EntityStats.newBuilder().setOid(1L).addStatSnapshots(snapshot1).build(),
                EntityStats.newBuilder().setOid(2L).addStatSnapshots(snapshot2).build()));
        assertThat(response.getPaginationResponse(), is(PaginationResponse.newBuilder()
                .setNextCursor("2")
                .setTotalRecordCount(3)
                .build()));
    }

    @Test
    public void testCalculateFinalPage() {
        final TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        final StatSnapshotCalculator snapshotCalculator = mock(StatSnapshotCalculator.class);

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(COMMODITY);
        when(paginationParams.getLimit()).thenReturn(2);
        when(paginationParams.getNextCursor()).thenReturn(Optional.empty());

        final Set<Long> entities = Sets.newHashSet(1L);
        final Set<String> commodities = Sets.newHashSet(COMMODITY);

        final StatSnapshot snapshot1 = StatSnapshot.newBuilder()
                .setSnapshotDate(1L)
                .build();
        when(snapshot.getEntityComparator(paginationParams,
            StatsTestUtils.createEntityGroupsMap(entities))).thenReturn(Long::compare);
        when(snapshotCalculator.buildSnapshot(snapshot, Collections.singleton(1L), commodities))
                .thenReturn(snapshot1);

        final EntityStatsCalculator entityStatsCalculator = new EntityStatsCalculator() {};
        final ProjectedEntityStatsResponse response = entityStatsCalculator.calculateNextPage(
            snapshot, snapshotCalculator, StatsTestUtils.createEntityGroupsMap(entities),
            commodities, paginationParams);
        assertThat(response.getEntityStatsList(), contains(
                EntityStats.newBuilder().setOid(1L).addStatSnapshots(snapshot1).build()));
        assertTrue(response.getPaginationResponse().hasTotalRecordCount());
        assertFalse(response.getPaginationResponse().hasNextCursor());
    }
}
