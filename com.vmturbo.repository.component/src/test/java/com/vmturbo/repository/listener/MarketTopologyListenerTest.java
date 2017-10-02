package com.vmturbo.repository.listener;

import static org.mockito.Matchers.any;
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

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufWriter;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;
import com.vmturbo.repository.util.RepositoryTestUtil;

/**
 * Unit tests for {@link TopologyEntitiesListener}.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class MarketTopologyListenerTest {

    private MarketTopologyListener marketTopologyListener;

    @Mock
    private TopologyEventHandler topologyEventHandler;

    @Mock
    private RepositoryNotificationSender apiBackend;

    @Mock
    TopologyProtobufsManager topologyProtobufsManager;

    @Mock
    TopologyProtobufWriter writer;

    @Mock
    private TopologyGraphCreator topologyGraphCreator;

    @Mock
    private RemoteIterator<TopologyEntityDTO> entityIterator;

    private final TopologyEntityDTO vmDTO;
    private final TopologyEntityDTO pmDTO;
    private final TopologyEntityDTO dsDTO;

    public MarketTopologyListenerTest() throws IOException {
        vmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        dsDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
    }

    @Before
    public void setUp() throws Exception {
        marketTopologyListener = new MarketTopologyListener(
                topologyEventHandler,
                apiBackend,
                topologyProtobufsManager);

        // Simulates three DTOs with two chunks received by the listener.
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenReturn(Sets.newHashSet(dsDTO));
        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyEventHandler.initializeTopologyGraph(any())).thenReturn(topologyGraphCreator);
        when(topologyProtobufsManager.createTopologyProtobufWriter(Mockito.anyLong())).thenReturn(writer);
    }

    /**
     * Test that the methods that need to be invoked are indeed invoked and with the right params.
     * @throws GraphDatabaseException is not expected to happen during this test
     */
    @Test
    public void testOnProjectedTopologyReceived() throws GraphDatabaseException {
        final long topologyContextId = 11L;
        final long srcTopologyId = 11111L;
        final long projectedTopologyId = 33333L;
        final long creationTime = 44444L;
        final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId, TopologyType.PROJECTED);
        marketTopologyListener.onProjectedTopologyReceived(
                projectedTopologyId,
                TopologyInfo.newBuilder()
                        .setTopologyId(srcTopologyId)
                        .setTopologyContextId(topologyContextId)
                        .setCreationTime(creationTime)
                        .build(),
                entityIterator);

        verify(topologyEventHandler).initializeTopologyGraph(tid);
        verify(topologyEventHandler).register(tid);
        verify(topologyEventHandler, never()).dropDatabase(tid);
        // 2 invocations, one for each chunk
        verify(topologyGraphCreator, times(2)).updateTopologyToDb(any());
        verify(writer, times(2)).storeChunk(any());
    }
}
