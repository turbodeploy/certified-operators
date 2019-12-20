package com.vmturbo.topology.processor.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineRequest;

/**
 * Unit test for {@link TopologyHandler}.
 */
public class TopologyHandlerTest {

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final long TIMEOUT_MS = 5;

    private TopologyHandler topologyHandler;

    private final long topologyId = 0L;

    private final long realtimeTopologyContextId = 7000;

    private final long clockTime = 77L;

    private final TopologyPipelineExecutorService pipelineExecutorService = mock(TopologyPipelineExecutorService.class);

    private final Clock clock = mock(Clock.class);

    private final EntityStore entityStore = mock(EntityStore.class);

    private final ProbeStore probeStore = mock(ProbeStore.class);

    private final TargetStore targetStore = mock(TargetStore.class);

    private final TopologyInfo realtimeTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setTopologyId(topologyId)
            .setCreationTime(clockTime)
            .setTopologyType(TopologyType.REALTIME)
            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
            .addAnalysisType(AnalysisType.BUY_RI_IMPACT_ANALYSIS)
            .addAnalysisType(AnalysisType.WASTED_FILES)
            .build();

    private final ProbeInfo awsProbeInfo = ProbeInfo.newBuilder()
        .setProbeType(SDKProbeType.AWS.getProbeType())
        .setProbeCategory(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
        .build();

    @Before
    public void init() {
        topologyHandler = new TopologyHandler(realtimeTopologyContextId, pipelineExecutorService,
                identityProvider, probeStore, targetStore, clock, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        when(clock.millis()).thenReturn(clockTime);
        when(probeStore.getProbe(1l)).thenReturn(Optional.of(awsProbeInfo));
        Target awsTarget = mock(Target.class);
        when(targetStore.getAll()).thenReturn(Collections.singletonList(awsTarget));
        when(awsTarget.getProbeId()).thenReturn(1l);
    }

    @Test
    public void testBroadcastTopology() throws Exception {
        final StitchingJournalFactory journalFactory = StitchingJournalFactory.emptyStitchingJournalFactory();
        TopologyBroadcastInfo broadcastInfo = mock(TopologyBroadcastInfo.class);
        TopologyPipelineRequest topologyPipelineRequest = mock(TopologyPipelineRequest.class);
        when(topologyPipelineRequest.waitForBroadcast(anyLong(), any())).thenReturn(broadcastInfo);
        when(pipelineExecutorService.queueLivePipeline(eq(realtimeTopologyInfo), eq(Collections.emptyList()),
                eq(journalFactory)))
            .thenReturn(topologyPipelineRequest);

        topologyHandler.broadcastLatestTopology(journalFactory);

        verify(topologyPipelineRequest).waitForBroadcast(5L, TimeUnit.MILLISECONDS);
        assertThat(topologyHandler.broadcastLatestTopology(journalFactory), is(broadcastInfo));
    }
}
