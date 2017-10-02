package com.vmturbo.repository.listener;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
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
 * Unit tests for {@link MarketTopologyListener} throwing exceptions.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class MarketTopologyListenerExceptionTest {

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

    private static final long topologyContextId = 11L;
    private static final long srcTopologyId = 11111L;
    private static final long projectedTopologyId = 33333L;
    private static final long creationTime = 44444L;
    private static final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId, TopologyType.PROJECTED);

    public MarketTopologyListenerExceptionTest() throws IOException {
        vmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        pmDTO = RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        marketTopologyListener = new MarketTopologyListener(
                topologyEventHandler,
                apiBackend,
                topologyProtobufsManager);

        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyEventHandler.initializeTopologyGraph(any())).thenReturn(topologyGraphCreator);
        when(topologyProtobufsManager.createTopologyProtobufWriter(Mockito.anyLong())).thenReturn(writer);
    }

    /**
     * Verify correct handling of {@link InterruptedException}.
     * @throws Exception when something goes wrong
     */
    @Test
    public void testOnProjectedTopologyReceivedInterruptedException() throws Exception {
        // Simulates one chunk and then an exception
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenThrow(new InterruptedException("interrupted"));
        marketTopologyListener.onProjectedTopologyReceived(
                projectedTopologyId,
                TopologyInfo.newBuilder()
                        .setTopologyId(srcTopologyId)
                        .setTopologyContextId(topologyContextId)
                        .setCreationTime(creationTime)
                        .build(),
                 entityIterator);

        verifyMocks();
        verify(apiBackend, never()).onProjectedTopologyFailure(
            eq(projectedTopologyId), eq(topologyContextId), any(String.class));
        verify(writer).delete();
    }

    /**
     * Verify correct handling of {@link CommunicationException}.
     * @throws Exception when something goes wrong
     */
    @Test
    public void testOnProjectedTopologyReceivedCommunicationException() throws Exception {
        // Simulates one chunk and then an exception
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenThrow(new CommunicationException("communication exception"));
        marketTopologyListener.onProjectedTopologyReceived(
                projectedTopologyId,
                TopologyInfo.newBuilder()
                        .setTopologyId(srcTopologyId)
                        .setTopologyContextId(topologyContextId)
                        .setCreationTime(creationTime)
                        .build(),
                entityIterator);

        verifyMocks();
        verify(apiBackend).onProjectedTopologyFailure(
            eq(projectedTopologyId), eq(topologyContextId), any(String.class));
        verify(writer).delete();
    }

    /**
     * Verify correct handling of {@link Exception}.
     * @throws Exception when something goes wrong
     */
    @Test
    public void testOnProjectedTopologyReceivedOtherExceptions() throws Exception {
        // Simulates one chunk and then an exception
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenThrow(new IllegalStateException("other exception"));
        try {
            marketTopologyListener.onProjectedTopologyReceived(
                    projectedTopologyId,
                    TopologyInfo.newBuilder()
                            .setTopologyId(srcTopologyId)
                            .setTopologyContextId(topologyContextId)
                            .setCreationTime(creationTime)
                            .build(),
                    entityIterator);
        } catch (IllegalStateException ise) {
            // expected
        }
        verifyMocks();
        verify(apiBackend, never()).onProjectedTopologyFailure(
            any(long.class), any(long.class), any(String.class));
        verify(writer).delete();
    }

    private void verifyMocks() throws GraphDatabaseException {
        verify(topologyEventHandler).initializeTopologyGraph(tid);
        verify(topologyEventHandler, never()).register(tid);
        verify(topologyEventHandler).dropDatabase(tid);
        verify(writer).storeChunk(any());
        // 1 invocation before the exception is thrown
        verify(topologyGraphCreator, times(1)).updateTopologyToDb(any());
        verify(apiBackend, never()).onProjectedTopologyAvailable(any(long.class), any(long.class));
    }
}
