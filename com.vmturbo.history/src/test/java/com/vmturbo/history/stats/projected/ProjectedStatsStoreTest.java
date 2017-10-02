package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;

public class ProjectedStatsStoreTest {

    private TopologyCommoditiesSnapshotFactory snapshotFactory =
            mock(TopologyCommoditiesSnapshotFactory.class);

    private ProjectedStatsStore store = new ProjectedStatsStore(snapshotFactory);

    @Captor
    private ArgumentCaptor<Collection<TopologyEntityDTO>> entitiesCaptor;

    @SuppressWarnings("unchecked")
    private RemoteIterator<TopologyEntityDTO> emptyIterator =
            (RemoteIterator<TopologyEntityDTO>)mock(RemoteIterator.class);



    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(emptyIterator.hasNext()).thenReturn(false);
    }

    @Test
    public void testGetSnapshot() throws Exception {
        final TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        when(snapshotFactory.createSnapshot(eq(emptyIterator)))
           .thenReturn(snapshot);

        store.updateProjectedTopology(emptyIterator);

        final StatRecord record = StatRecord.newBuilder()
                .setName(COMMODITY)
                .build();

        when(snapshot.getRecords(eq(Collections.singleton(COMMODITY)),
                eq(Collections.singleton(10L))))
                .thenReturn(Stream.of(record));

        StatSnapshot result = store.getStatSnapshot(ProjectedStatsRequest.newBuilder()
                    .addCommodityName(COMMODITY)
                    .addEntities(10L)
                    .build())
                // Expect a result
                .get();

        assertFalse(result.hasStartDate());
        assertFalse(result.hasEndDate());
        assertFalse(result.hasSnapshotDate());
        assertEquals(1, result.getStatRecordsCount());
        assertEquals(record, result.getStatRecords(0));
    }

    @Test
    public void testGetSnapshotNoData() {
        // Initially the store has no data.
        assertFalse(store.getStatSnapshot(ProjectedStatsRequest.getDefaultInstance()).isPresent());
    }

    @Test
    public void testUpdateSnapshot() throws Exception {
        final TopologyEntityDTO entity =
                TopologyEntityDTO.newBuilder()
                    .setEntityType(1)
                    .setOid(10)
                    .build();

        @SuppressWarnings("unchecked")
        final RemoteIterator<TopologyEntityDTO> remoteIterator =
                (RemoteIterator<TopologyEntityDTO>)Mockito.mock(RemoteIterator.class);

        when(remoteIterator.hasNext()).thenReturn(true, false);
        when(remoteIterator.nextChunk()).thenReturn(Collections.singletonList(entity));

        TopologyCommoditiesSnapshot snapshot = mock(TopologyCommoditiesSnapshot.class);
        when(snapshot.getTopologySize()).thenReturn(7L);

        when(snapshotFactory.createSnapshot(eq(remoteIterator)))
               .thenReturn(snapshot);

        assertEquals(7L, store.updateProjectedTopology(remoteIterator));

        verify(snapshotFactory).createSnapshot(eq(remoteIterator));
    }

    @Test
    public void testPriceIndex() {
        store.updateProjectedPriceIndex(PriceIndexMessage.newBuilder()
            .addPayload(PriceIndexMessagePayload.newBuilder()
                .setOid(1)
                .setPriceindexCurrent(1)
                .setPriceindexProjected(10))
            .build());

        assertEquals(Double.valueOf(10), store.getPriceIndex(1).get());
    }
}
