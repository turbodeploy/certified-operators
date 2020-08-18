package com.vmturbo.repository.listener;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
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
public class MarketTopologyListenerTest {

    private final long realtimeContextId = 10231;
    private MarketTopologyListener marketTopologyListener;

    @Mock
    private TopologyLifecycleManager topologyManager;

    @Mock
    private RepositoryNotificationSender apiBackend;

    @Mock
    private TopologyCreator<ProjectedTopologyEntity> topologyCreator;

    @Mock
    private LiveTopologyStore liveTopologyStore;

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
        marketTopologyListener = new MarketTopologyListener(apiBackend, topologyManager);

        // Simulates three DTOs with two chunks received by the listener.
        when(entityIterator.nextChunk()).thenReturn(Sets.newHashSet(vmDTO, pmDTO))
                                        .thenReturn(Sets.newHashSet(dsDTO));
        when(entityIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(topologyManager.newProjectedTopologyCreator(any(), any())).thenReturn(topologyCreator);
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
        when(topologyManager.getRealtimeTopologyId()).thenReturn(Optional.empty());
        final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId, TopologyID.TopologyType.PROJECTED);
        final TopologyInfo originalTopoInfo = TopologyInfo.newBuilder()
            .setTopologyId(srcTopologyId)
            .setTopologyContextId(topologyContextId)
            .setCreationTime(creationTime)
            .build();

        marketTopologyListener.onProjectedTopologyReceived(projectedTopologyId, originalTopoInfo,
                entityIterator, Mockito.mock(SpanContext.class));

        verify(topologyManager).newProjectedTopologyCreator(tid, originalTopoInfo);
        verify(topologyCreator).complete();
        verify(topologyCreator, never()).rollback();
        // 2 invocations, one for each chunk
        verify(topologyCreator, times(2)).addEntities(any());
    }

    @Test
    public void testOnStaleProjectedTopologyReceived() throws Exception {
        // verify that the projected topology will get skipped if a newer projected topolgy has
        // been received. In this case we skip since id 3 is "bigger" than id 2.
        final long topologyContextId = 11L;
        final long srcTopologyId = 1;
        final long projectedTopologyId = 2L;
        final long creationTime = 44444L;

        marketTopologyListener.onAnalysisSummary(AnalysisSummary.newBuilder()
            .setProjectedTopologyInfo(
                ProjectedTopologyInfo.newBuilder()
                    .setProjectedTopologyId(3L)
                    .build()
            ).build());

        final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId, TopologyID.TopologyType.PROJECTED);
        final TopologyInfo originalTopoInfo = TopologyInfo.newBuilder()
            .setTopologyId(srcTopologyId)
            .setTopologyContextId(topologyContextId)
            .setCreationTime(creationTime)
            .build();

        marketTopologyListener.onProjectedTopologyReceived(projectedTopologyId,
                originalTopoInfo,
                entityIterator,
                Mockito.mock(SpanContext.class));

        // should not have any "write" interactions
        verify(topologyManager, never()).newProjectedTopologyCreator(tid, originalTopoInfo);
        verify(topologyCreator, never()).complete();
    }
}
