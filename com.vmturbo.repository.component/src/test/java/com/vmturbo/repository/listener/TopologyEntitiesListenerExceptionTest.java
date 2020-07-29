package com.vmturbo.repository.listener;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
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
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.util.RepositoryTestUtil;

/**
 * Unit test for {@link TopologyEntitiesListener} that throws an exception
 * while getting the chunks.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyEntitiesListenerExceptionTest {

    private TopologyEntitiesListener topologyEntitiesListener;

    @Mock
    private GraphDatabaseDriver graphDatabaseBuilder;

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

    public TopologyEntitiesListenerExceptionTest() throws IOException {
        TopologyEntityDTO vmDTOE = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        TopologyEntityDTO pmDTOE = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        vmDTO = TopologyDTO.Topology.DataSegment.newBuilder().setEntity(vmDTOE).build();
        pmDTO = TopologyDTO.Topology.DataSegment.newBuilder().setEntity(pmDTOE).build();
    }

    @Before
    public void setUp() throws Exception {
        topologyEntitiesListener = new TopologyEntitiesListener(
                topologyManager,
                liveTopologyStore,
                notificationSender,
                e -> true);

        // Simulates one chunk and then an exception
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenThrow(GraphDatabaseException.class);
        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    }

    @Test
    public void testOnTopologyNotificationRealtime() throws Exception {
        final long topologyContextId = realtimeTopologyContextId;
        final long topologyId = 22222L;
        final long creationTime = 33333L;
        final TopologyID tid = new TopologyID(topologyContextId, topologyId,
                TopologyID.TopologyType.SOURCE);
        final TopologyInfo tInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyId(topologyId)
            .setCreationTime(creationTime)
            .build();

        when(topologyManager.newSourceTopologyCreator(eq(tid), eq(tInfo))).thenReturn(topologyCreator);

        topologyEntitiesListener.onTopologyNotification(tInfo, entityIterator, Mockito.mock(SpanContext.class));

        verify(topologyCreator).initialize();
        verify(topologyCreator, never()).complete();
        verify(topologyCreator, times(1)).rollback();
        verify(topologyCreator, times(1)).addEntities(any());
        verify(notificationSender, never()).onSourceTopologyAvailable(anyLong(), anyLong());
        verify(notificationSender).onSourceTopologyFailure(anyLong(), anyLong(), anyString());
    }

}
