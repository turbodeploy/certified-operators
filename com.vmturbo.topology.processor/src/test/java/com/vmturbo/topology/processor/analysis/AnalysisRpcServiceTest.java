package com.vmturbo.topology.processor.analysis;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;

/**
 * Unit tests for the {@link AnalysisRpcService}.
 */
public class AnalysisRpcServiceTest {

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private Clock clock = mock(Clock.class);

    private EntityStore entityStore = mock(EntityStore.class);

    private TopologyPipelineFactory pipelineFactory = mock(TopologyPipelineFactory.class);

    private AnalysisRpcService analysisService =
            new AnalysisRpcService(pipelineFactory, identityProvider, entityStore, clock);

    private final long returnEntityNum = 1337;

    private final long planId = 123;

    private final long topologyId = 10;

    private final long topologyContextId = 11;

    private final long clockTime = 7L;

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(planId)
        .setTopologyId(topologyId)
        .setCreationTime(clockTime)
        .setTopologyType(TopologyType.PLAN)
        .setPlanInfo(PlanTopologyInfo.newBuilder()
            .setPlanType(PlanProjectType.USER))
        .build();

    private TopologyBroadcastInfo broadcastInfo = mock(TopologyBroadcastInfo.class);

    @Before
    public void setup() throws IOException, InterruptedException, CommunicationException {
        MockitoAnnotations.initMocks(this);
        when(broadcastInfo.getEntityCount()).thenReturn(returnEntityNum);
        when(broadcastInfo.getTopologyId()).thenReturn(topologyId);
        when(broadcastInfo.getTopologyContextId()).thenReturn(topologyContextId);

        when(clock.millis()).thenReturn(clockTime);
    }

    /**
     * Test running an analysis on a non-realtime topology.
     * @throws InterruptedException not supposed to happen
     */
    @Test
    public void testStartAnalysisOldTopology() throws Exception {
        // arrange
        final TopologyPipeline<Long, TopologyBroadcastInfo> planOverPlanPipeline =
                (TopologyPipeline<Long, TopologyBroadcastInfo>)mock(TopologyPipeline.class);
        when(planOverPlanPipeline.run(eq(topologyId)))
                .thenReturn(broadcastInfo);
        when(pipelineFactory.planOverOldTopology(eq(topologyInfo), eq(Collections.emptyList()), any()))
                .thenReturn(planOverPlanPipeline);

        // act
        StreamObserver<StartAnalysisResponse> responseObserver = mock(StreamObserver.class);

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                    .setPlanId(planId)
                    // Set the topology ID to request a specific topology.
                    .setTopologyId(topologyId)
                    .build(), responseObserver);

        final ArgumentCaptor<StartAnalysisResponse> responseCaptor =
                ArgumentCaptor.forClass(StartAnalysisResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        final StartAnalysisResponse response = responseCaptor.getValue();
        assertThat(response.getEntitiesBroadcast(), is(broadcastInfo.getEntityCount()));
        assertThat(response.getTopologyContextId(), is(broadcastInfo.getTopologyContextId()));
        assertThat(response.getTopologyId(), is(broadcastInfo.getTopologyId()));
    }

    /**
     * Test running an analysis on the realtime topology.
     * @throws InterruptedException not supposed to happen
     */
    @Test
    public void testStartAnalysisPlan() throws Exception {
        // arrange
        final TopologyPipeline<EntityStore, TopologyBroadcastInfo> planPipeline =
                (TopologyPipeline<EntityStore, TopologyBroadcastInfo>)mock(TopologyPipeline.class);
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        when(planPipeline.run(eq(entityStore)))
                .thenReturn(broadcastInfo);
        when(pipelineFactory.planOverLiveTopology(eq(topologyInfo), eq(Collections.emptyList()), any()))
                .thenReturn(planPipeline);

        // act
        StreamObserver<StartAnalysisResponse> responseObserver = mock(StreamObserver.class);

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                // Don't set topology ID.
                .build(), responseObserver);

        verify(pipelineFactory).planOverLiveTopology(eq(topologyInfo), eq(Collections.emptyList()), any());

        final ArgumentCaptor<StartAnalysisResponse> responseCaptor =
                ArgumentCaptor.forClass(StartAnalysisResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        final StartAnalysisResponse response = responseCaptor.getValue();
        assertThat(response.getEntitiesBroadcast(), is(broadcastInfo.getEntityCount()));
        assertThat(response.getTopologyContextId(), is(broadcastInfo.getTopologyContextId()));
        assertThat(response.getTopologyId(), is(broadcastInfo.getTopologyId()));
    }
}
