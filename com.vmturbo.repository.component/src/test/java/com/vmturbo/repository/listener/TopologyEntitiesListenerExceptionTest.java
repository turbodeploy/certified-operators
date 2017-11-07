package com.vmturbo.repository.listener;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
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

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;
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
    private TopologyEventHandler topologyEventHandler;

    @Mock
    private TopologyRelationshipRecorder globalSupplyChainRecorder;

    @Mock
    private TopologyGraphCreator topologyGraphCreator;

    private final long realtimeTopologyContextId = 77L;

    @Mock
    private RemoteIterator<TopologyEntityDTO> entityIterator;

    @Mock
    private RepositoryNotificationSender notificationSender;

    private final TopologyEntityDTO vmDTO;
    private final TopologyEntityDTO pmDTO;

    public TopologyEntitiesListenerExceptionTest() throws IOException {
        vmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
    }

    @Before
    public void setUp() throws Exception {
        topologyEntitiesListener = new TopologyEntitiesListener(
                topologyEventHandler,
                globalSupplyChainRecorder,
                notificationSender);

        // Simulates one chunk and then an exception
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenThrow(GraphDatabaseException.class);
        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyEventHandler.initializeTopologyGraph(any())).thenReturn(topologyGraphCreator);
    }

    @Test
    public void testOnTopologyNotificationRealtime() throws Exception {
        final long topologyContextId = realtimeTopologyContextId;
        final long topologyId = 22222L;
        final long creationTime = 33333L;
        final TopologyID tid = new TopologyID(topologyContextId, topologyId, TopologyType.SOURCE);

        topologyEntitiesListener.onTopologyNotification(
                TopologyInfo.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setTopologyId(topologyId)
                        .setCreationTime(creationTime)
                        .build(),
                entityIterator);

        verify(topologyEventHandler).initializeTopologyGraph(tid);
        verify(topologyEventHandler, never()).register(tid);
        verify(topologyEventHandler, times(1)).dropDatabase(tid);
        verify(topologyGraphCreator, times(1)).updateTopologyToDb(any());
        verify(globalSupplyChainRecorder, never()).setGlobalSupplyChainProviderRels(any());
        verify(notificationSender, never()).onSourceTopologyAvailable(anyLong(), anyLong());
        verify(notificationSender).onSourceTopologyFailure(anyLong(), anyLong(), anyString());
    }

}
