package com.vmturbo.repository.listener;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.util.RepositoryTestUtil;

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

    @Mock
    private LiveTopologyStore liveTopologyStore;

    private final TopologyDTO.Topology.DataSegment vmDTO;
    private final TopologyDTO.Topology.DataSegment pmDTO;
    private final TopologyDTO.Topology.DataSegment dsDTO;

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
        topologyEntitiesListener = new TopologyEntitiesListener(
                topologyManager,
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
}
