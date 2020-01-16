package com.vmturbo.topology.processor.analysis;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.List;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineRequest;

/**
 * Unit tests for the {@link AnalysisRpcService}.
 */
public class AnalysisRpcServiceTest {

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private Clock clock = mock(Clock.class);

    private TopologyPipelineExecutorService pipelineExecutorService = mock(TopologyPipelineExecutorService.class);

    private final TopologyHandler topologyHandler = mock(TopologyHandler.class);

    private StitchingJournalFactory journalFactory = StitchingJournalFactory.emptyStitchingJournalFactory();

    private AnalysisRpcService analysisService =
            new AnalysisRpcService(pipelineExecutorService, topologyHandler, identityProvider,
                journalFactory, clock);

    private final long returnEntityNum = 1337;

    private final long planId = 123;

    private final long topologyId = 10;

    private final long topologyContextId = 11;

    private final long clockTime = 7L;

    private final String testPlanType = "TEST_PLAN";

    private final String testPlanSubType = "TEST_PLAN_SUBTYPE";

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(planId)
        .setTopologyId(topologyId)
        .setCreationTime(clockTime)
        .setTopologyType(TopologyType.PLAN)
        .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
        .setPlanInfo(PlanTopologyInfo.newBuilder()
            .setPlanProjectType(PlanProjectType.USER)
            .setPlanType(testPlanType)
            .setPlanSubType(testPlanSubType))
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
        final long retTopologyId = 7129;
        final TopologyPipelineRequest req = mock(TopologyPipelineRequest.class);
        when(req.getTopologyId()).thenReturn(retTopologyId);
        when(pipelineExecutorService.queuePlanOverPlanPipeline(eq(topologyId), eq(topologyInfo), eq(Collections.emptyList()), any()))
            .thenReturn(req);

        // act
        StreamObserver<StartAnalysisResponse> responseObserver = mock(StreamObserver.class);

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                    .setPlanId(planId)
                    // Set the topology ID to request a specific topology.
                    .setTopologyId(topologyId)
                    .setPlanProjectType(PlanProjectType.USER)
                    .setPlanType(testPlanType)
                    .setPlanSubType(testPlanSubType)
                    .build(), responseObserver);

        verify(responseObserver).onNext(StartAnalysisResponse.newBuilder()
            .setTopologyId(retTopologyId)
            .build());
        verify(responseObserver).onCompleted();
    }

    /**
     * Test running an analysis on the realtime topology.
     * @throws InterruptedException not supposed to happen
     */
    @Test
    public void testStartAnalysisPlan() throws Exception {
        // arrange
        final long retTopologyId = 7129;
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        final TopologyPipelineRequest topologyPipelineRequest = mock(TopologyPipelineRequest.class);
        when(topologyPipelineRequest.getTopologyId()).thenReturn(retTopologyId);
        when(pipelineExecutorService.queuePlanPipeline(eq(topologyInfo), eq(Collections.emptyList()), any(),
            any(StitchingJournalFactory.class)))
            .thenReturn(topologyPipelineRequest);

        // act
        StreamObserver<StartAnalysisResponse> responseObserver = mock(StreamObserver.class);

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                .setPlanProjectType(PlanProjectType.USER)
                .setPlanType(testPlanType)
                .setPlanSubType(testPlanSubType)
                // Don't set topology ID.
                .build(), responseObserver);

        verify(pipelineExecutorService).queuePlanPipeline(eq(topologyInfo), eq(Collections.emptyList()),
            any(), any(StitchingJournalFactory.class));

        verify(responseObserver).onNext(StartAnalysisResponse.newBuilder()
            .setTopologyId(retTopologyId)
            .build());
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testWastedFilesAnalysisIncludedForAddWorkloadPlan() throws Exception {
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        final TopologyPipelineRequest topologyPipelineRequest = mock(TopologyPipelineRequest.class);
        when(pipelineExecutorService.queuePlanPipeline(any(), any(), any(),
            any(StitchingJournalFactory.class))).thenReturn(topologyPipelineRequest);
        // include wasted files (there are wasted files related targets)
        when(topologyHandler.includesWastedFiles()).thenReturn(true);

        StartAnalysisRequest request = StartAnalysisRequest.newBuilder()
            .setPlanId(planId)
            .setPlanProjectType(PlanProjectType.USER)
            .setPlanType("ADD_WORKLOAD")
            .build();
        analysisService.startAnalysis(request, mock(StreamObserver.class));

        // capture argument
        final ArgumentCaptor<TopologyInfo> responseCaptor = ArgumentCaptor.forClass(TopologyInfo.class);
        verify(pipelineExecutorService).queuePlanPipeline(responseCaptor.capture(),
            isA(List.class),
            isA(PlanScope.class),
            isA(StitchingJournalFactory.class));

        // verify it includes wasted files analysis
        final TopologyInfo response = responseCaptor.getValue();
        assertThat(response.getAnalysisTypeCount(), is(2));
        assertThat(response.getAnalysisTypeList(), containsInAnyOrder(AnalysisType.MARKET_ANALYSIS,
            AnalysisType.WASTED_FILES));
    }

    @Test
    public void testWastedFilesAnalysisNotIncludedForCloudMigrationPlan() throws Exception {
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        final TopologyPipelineRequest topologyPipelineRequest = mock(TopologyPipelineRequest.class);
        when(pipelineExecutorService.queuePlanPipeline(any(), any(), any(),
            any(StitchingJournalFactory.class))).thenReturn(topologyPipelineRequest);
        // include wasted files (there are wasted files related targets)
        when(topologyHandler.includesWastedFiles()).thenReturn(true);

        StartAnalysisRequest request = StartAnalysisRequest.newBuilder()
            .setPlanId(planId)
            .setPlanProjectType(PlanProjectType.USER)
            .setPlanType(StringConstants.CLOUD_MIGRATION_PLAN)
            .build();
        analysisService.startAnalysis(request, mock(StreamObserver.class));

        // capture argument
        final ArgumentCaptor<TopologyInfo> responseCaptor = ArgumentCaptor.forClass(TopologyInfo.class);
        verify(pipelineExecutorService).queuePlanPipeline(responseCaptor.capture(),
            isA(List.class),
            isA(PlanScope.class),
            isA(StitchingJournalFactory.class));

        // verify it doesn't include wasted files analysis
        final TopologyInfo response = responseCaptor.getValue();
        assertThat(response.getAnalysisTypeCount(), is(1));
        assertThat(response.getAnalysisType(0), is(AnalysisType.MARKET_ANALYSIS));
    }
}
