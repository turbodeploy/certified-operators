package com.vmturbo.repository.listener;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.util.RepositoryTestUtil;

/**
 * Unit tests for {@link TopologyEntitiesListener}.
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class MarketTopologyListenerTest {

    private MarketTopologyListener marketTopologyListener;

    @Mock
    private TopologyLifecycleManager topologyManager;

    @Mock
    private RepositoryNotificationSender apiBackend;

    @Mock
    private TopologyCreator topologyCreator;

    @Mock
    private RemoteIterator<ProjectedTopologyEntity> entityIterator;

    private final ProjectedTopologyEntity vmDTO;
    private final ProjectedTopologyEntity pmDTO;
    private final ProjectedTopologyEntity dsDTO;

    public MarketTopologyListenerTest() throws IOException {
        vmDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(RepositoryTestUtil.messageFromJsonFile("protobuf/messages/vm-1.dto.json"))
            .build();
        pmDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(RepositoryTestUtil.messageFromJsonFile("protobuf/messages/pm-1.dto.json"))
            .build();
        dsDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(RepositoryTestUtil.messageFromJsonFile("protobuf/messages/ds-1.dto.json"))
            .build();
    }

    @Before
    public void setUp() throws Exception {
        marketTopologyListener = new MarketTopologyListener(
                apiBackend,
                topologyManager);

        // Simulates three DTOs with two chunks received by the listener.
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenReturn(Sets.newHashSet(dsDTO));
        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyManager.newTopologyCreator(any())).thenReturn(topologyCreator);
    }

    /**
     * Test that the methods that need to be invoked are indeed invoked and with the right params.
     */
    @Test
    public void testOnProjectedTopologyReceived() throws Exception {
        final long topologyContextId = 11L;
        final long srcTopologyId = 11111L;
        final long projectedTopologyId = 33333L;
        final long creationTime = 44444L;
        final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId, TopologyID.TopologyType.PROJECTED);
        marketTopologyListener.onProjectedTopologyReceived(
                projectedTopologyId,
                TopologyInfo.newBuilder()
                        .setTopologyId(srcTopologyId)
                        .setTopologyContextId(topologyContextId)
                        .setCreationTime(creationTime)
                        .build(),
                Collections.emptySet(),
                entityIterator);

        verify(topologyManager).newTopologyCreator(tid);
        verify(topologyCreator).complete();
        verify(topologyCreator, never()).rollback();
        // 2 invocations, one for each chunk
        verify(topologyCreator, times(2)).addEntities(any());
    }
}
