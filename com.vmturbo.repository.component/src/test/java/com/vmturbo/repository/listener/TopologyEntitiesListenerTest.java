package com.vmturbo.repository.listener;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.util.RepositoryTestUtil;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * Unit tests for {@link TopologyEntitiesListener}.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyEntitiesListenerTest {

    private TopologyEntitiesListener topologyEntitiesListener;

    @Mock
    private TopologyLifecycleManager topologyManager;

    @Mock
    private TopologyCreator<TopologyEntityDTO> topologyCreator;

    private final long realtimeTopologyContextId = 77L;

    @Mock
    private RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator;

    @Mock
    private RepositoryNotificationSender notificationSender;


    private LiveTopologyStore liveTopologyStore;

    private final TopologyDTO.Topology.DataSegment vmDTO;
    private final TopologyDTO.Topology.DataSegment pmDTO;
    private final TopologyDTO.Topology.DataSegment dsDTO;
    private final Long hostId = 1L;
    private final TopologyEntityDTO host = TopologyEntityDTO.newBuilder()
        .setOid(hostId)
        .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
        .setEntityState(EntityState.POWERED_ON)
        .build();

    public TopologyEntitiesListenerTest() throws IOException {
        TopologyEntityDTO vmDTOE;
        TopologyEntityDTO pmDTOE;
        TopologyEntityDTO dsDTOE;

        vmDTOE = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmDTOE = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        dsDTOE = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json");

        vmDTO = TopologyDTO.Topology.DataSegment.newBuilder().setEntity(vmDTOE).build();
        pmDTO = TopologyDTO.Topology.DataSegment.newBuilder().setEntity(pmDTOE).build();
        dsDTO = TopologyDTO.Topology.DataSegment.newBuilder().setEntity(dsDTOE).build();
    }

    @Before
    public void setUp() throws Exception {
        liveTopologyStore =
            new LiveTopologyStore(new GlobalSupplyChainCalculator());

        topologyEntitiesListener = new TopologyEntitiesListener(
                topologyManager,
                liveTopologyStore,
                notificationSender);

        // Simulates three DTOs with two chunks received by the listener.
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenReturn(Sets.newHashSet(dsDTO));
        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyManager.newSourceTopologyCreator(any(), any())).thenReturn(topologyCreator);
    }

    @Test
    public void testOnTopologyNotificationRealtime() throws Exception {
        final long topologyContextId = realtimeTopologyContextId;
        final long topologyId = 22222L;
        final long creationTime = 33333L;
        final TopologyID tid = new TopologyID(topologyContextId, topologyId,
                TopologyID.TopologyType.SOURCE);
        final TopologyInfo info = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyId(topologyId)
            .setCreationTime(creationTime)
            .build();

        topologyEntitiesListener.onTopologyNotification(info, entityIterator);

        verify(topologyManager).newSourceTopologyCreator(tid, info);
        verify(topologyCreator).complete();
        verify(topologyCreator, times(2)).addEntities(any());
        verify(notificationSender).onSourceTopologyAvailable(eq(topologyId), eq(topologyContextId));
    }

    @Test
    public void testOnStaleTopology() throws Exception {
        // verify that the realtime topology processor will skip an incoming realtime topology if
        // it's "stale".
        // first we'll prime the topology listener with a "newer" topology summary.
        topologyEntitiesListener.onTopologySummary(TopologySummary.newBuilder()
            .setTopologyInfo(TopologyInfo.newBuilder()
                .setTopologyId(2L) // "2" will be newer than "1"
                .setCreationTime(2)
                .setTopologyContextId(realtimeTopologyContextId))
            .setSuccess(TopologyBroadcastSuccess.getDefaultInstance())
            .build());
        final long topologyId = 1L;
        final TopologyID tid = new TopologyID(realtimeTopologyContextId, topologyId,
                TopologyID.TopologyType.SOURCE);

        final TopologyInfo info = TopologyInfo.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setCreationTime(1)
            .setTopologyId(topologyId)
            .build();

        topologyEntitiesListener.onTopologyNotification(info, entityIterator);

        verify(topologyManager, never()).newSourceTopologyCreator(tid, info);
        verify(topologyCreator, never()).complete();
    }

    /**
     * Tests that the event onHostStateChange correctly updates the real time topology and sets
     * the new entity state.
     */
    @Test
    public void testOnEntitiesWithNewState() {
        TopologyInfo tInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .build();
        final SourceRealtimeTopologyBuilder bldr = liveTopologyStore.newRealtimeTopology(tInfo);
        bldr.addEntities(Collections.singletonList(host));
        bldr.finish();
        // Host goes into maintenance, with associated id = 1
        topologyEntitiesListener.onEntitiesWithNewState(EntitiesWithNewState
            .newBuilder().addTopologyEntity(TopologyEntityDTO.newBuilder()
                .setOid(hostId).setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.MAINTENANCE).build())
            .setStateChangeId(2)
            .build());
        // Make sure host is in maintenance
        final SourceRealtimeTopology topo = liveTopologyStore.getSourceTopology().get();
        Assert.assertEquals(EntityState.MAINTENANCE,
            topo.entityGraph().getEntity(host.getOid()).get().getEntityState());

        TopologyInfo tInfo1 = TopologyInfo.newBuilder()
            .setTopologyId(3)
            .build();
        // New topology gets ingested, with a new state for the host
        final SourceRealtimeTopologyBuilder bldr1 = liveTopologyStore.newRealtimeTopology(tInfo1);
        bldr1.addEntities(Collections.singletonList(host));
        bldr1.finish();
        // Make sure the host state is no longer updated with maintenance, but it has the new
        // powered_on state
        final SourceRealtimeTopology topo1 = liveTopologyStore.getSourceTopology().get();
        Assert.assertEquals(EntityState.POWERED_ON,
            topo1.entityGraph().getEntity(host.getOid()).get().getEntityState());
    }
}
