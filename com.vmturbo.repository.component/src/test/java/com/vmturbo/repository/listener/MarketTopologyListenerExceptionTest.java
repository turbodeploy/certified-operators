package com.vmturbo.repository.listener;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.util.RepositoryTestUtil;

/**
 * Unit tests for {@link MarketTopologyListener} throwing exceptions.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class MarketTopologyListenerExceptionTest {

    private MarketTopologyListener marketTopologyListener;

    @Mock
    private RepositoryNotificationSender apiBackend;

    @Mock
    private TopologyLifecycleManager topologyManager;

    @Mock
    private TopologyCreator topologyCreator;

    @Mock
    private RemoteIterator<ProjectedTopologyEntity> entityIterator;

    private final ProjectedTopologyEntity vmDTO;
    private final ProjectedTopologyEntity pmDTO;

    private static final long topologyContextId = 11L;
    private static final long srcTopologyId = 11111L;
    private static final long projectedTopologyId = 33333L;
    private static final long creationTime = 44444L;
    private static final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId, TopologyID.TopologyType.PROJECTED);

    public MarketTopologyListenerExceptionTest() throws IOException {
        vmDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json"))
            .build();
        pmDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json"))
            .build();
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        marketTopologyListener = new MarketTopologyListener( apiBackend, topologyManager);

        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyManager.newTopologyCreator(any())).thenReturn(topologyCreator);
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
                 Collections.emptySet(),
                 entityIterator);

        verifyMocks();
        verify(apiBackend, never()).onProjectedTopologyFailure(
            eq(projectedTopologyId), eq(topologyContextId), any(String.class));
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
                Collections.emptySet(),
                entityIterator);

        verifyMocks();
        verify(apiBackend).onProjectedTopologyFailure(
            eq(projectedTopologyId), eq(topologyContextId), any(String.class));
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
                    Collections.emptySet(),
                    entityIterator);
        } catch (IllegalStateException ise) {
            // expected
        }
        verifyMocks();
        verify(apiBackend, never()).onProjectedTopologyFailure(
            any(long.class), any(long.class), any(String.class));
    }

    private void verifyMocks() throws Exception {
        verify(topologyManager).newTopologyCreator(tid);
        verify(topologyCreator, never()).complete();
        verify(topologyCreator).rollback();
        // 1 invocation before the exception is thrown
        verify(topologyCreator).addEntities(any());
        verify(apiBackend, never()).onProjectedTopologyAvailable(any(long.class), any(long.class));
    }
}
