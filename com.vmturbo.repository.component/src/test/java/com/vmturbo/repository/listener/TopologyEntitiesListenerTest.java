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
 * Unit tests for {@link TopologyEntitiesListener}.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyEntitiesListenerTest {

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
    private final TopologyEntityDTO dsDTO;

    public TopologyEntitiesListenerTest() throws IOException {
        vmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        dsDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
    }

    @Before
    public void setUp() throws Exception {
        topologyEntitiesListener = new TopologyEntitiesListener(
                topologyEventHandler,
                globalSupplyChainRecorder,
                notificationSender);

        // Simulates three DTOs with two chunks received by the listener.
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenReturn(Sets.newHashSet(dsDTO));
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
        verify(topologyEventHandler).register(tid);
        verify(topologyGraphCreator, times(2)).updateTopologyToDb(any());
        verify(globalSupplyChainRecorder, times(1))
                .setGlobalSupplyChainProviderRels(any());
        verify(notificationSender).onSourceTopologyAvailable(eq(topologyId), eq(topologyContextId));
    }
}
