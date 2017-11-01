package com.vmturbo.topology.processor.analysis;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.TopologyHandler.TopologyBroadcastInfo;

/**
 * Unit tests for the {@link AnalysisService}.
 */
public class AnalysisServiceTest {

    private TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);

    private EntityStore entityStore = Mockito.mock(EntityStore.class);

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);

    private RepositoryClient repository = Mockito.mock(RepositoryClient.class);

    private Clock clock = Mockito.mock(Clock.class);

    private AnalysisService analysisServiceBackend =
            new AnalysisService(topologyHandler, entityStore, identityProvider, repository, clock);

    private AnalysisServiceBlockingStub analysisService;

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
        .build();

    @Captor
    private ArgumentCaptor<Stream<TopologyEntityDTO>> broadcastCaptor;

    private final long entityId = 10;

    private final TopologyEntityDTO.Builder testEntity = TopologyEntityDTO.newBuilder()
            .setOid(entityId)
            .setDisplayName("test")
            .setEntityType(1);
    private final TopologyEntityDTO.Builder testEntity2 = TopologyEntityDTO.newBuilder()
            .setOid(entityId + 1)
            .setDisplayName("test2")
            .setEntityType(2);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(analysisServiceBackend);

    @Before
    public void setup() throws IOException, InterruptedException {
        MockitoAnnotations.initMocks(this);

        analysisService = AnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel());

        final TopologyBroadcastInfo broadcastInfo = Mockito.mock(TopologyBroadcastInfo.class);
        when(broadcastInfo.getEntityCount()).thenReturn(returnEntityNum);
        when(broadcastInfo.getTopologyId()).thenReturn(topologyId);
        when(broadcastInfo.getTopologyContextId()).thenReturn(topologyContextId);

        // Return a hard-coded number. It doesn't really matter
        // what the number is, since it just comes back in the gRPC response.
        when(topologyHandler.broadcastTopology(any(), any()))
                .thenReturn(broadcastInfo);
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        when(clock.millis()).thenReturn(clockTime);
    }

    /**
     * Test running an analysis on a non-realtime topology.
     * @throws InterruptedException not supposed to happen
     */
    @Test
    public void testStartAnalysisOldTopology() throws InterruptedException {

        // arrange
        final long oldTopologyId = 1;
        List<TopologyEntityDTO> entities = ImmutableList.of(testEntity.build(), testEntity2.build());
        Collection<RepositoryDTO.RetrieveTopologyResponse> chunks = ImmutableList.of(
            RepositoryDTO.RetrieveTopologyResponse.newBuilder()
                .addAllEntities(entities)
                .build());
        when(repository.retrieveTopology(anyLong())).thenReturn(chunks.iterator());

        // act
        StartAnalysisResponse response =
                analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                        .setPlanId(planId)
                        // Set the topology ID to request a specific topology.
                        .setTopologyId(oldTopologyId)
                        .build());

        // assert
        verify(topologyHandler).broadcastTopology(eq(topologyInfo.toBuilder()
                    .setTopologyId(oldTopologyId)
                    .build()),
                broadcastCaptor.capture());
        assertEquals(returnEntityNum, response.getEntitiesBroadcast());
        assertEquals(entities, broadcastCaptor.getValue().collect(Collectors.toList()));
    }

    /**
     * Test running an analysis on the realtime topology.
     * @throws InterruptedException not supposed to happen
     */
    @Test
    public void testStartAnalysisRealtime() throws InterruptedException {
        Map<Long, TopologyEntityDTO.Builder> entities =
                ImmutableMap.of(10L, TopologyEntityDTO.newBuilder()
                    .setEntityType(1)
                    .setOid(10));

        when(entityStore.constructTopology()).thenReturn(entities);

        StartAnalysisResponse response =
                analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                    .setPlanId(planId)
                    // Don't set topology ID
                    .build());

        verify(entityStore).constructTopology();
        verify(topologyHandler).broadcastTopology(eq(topologyInfo), broadcastCaptor.capture());

        assertEquals(entities.values().stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toList()), broadcastCaptor.getValue().collect(Collectors.toList()));
        assertEquals(returnEntityNum, response.getEntitiesBroadcast());

        // Verify the analysis service passes through the topologyId and topologyContextId.
        assertEquals(topologyId, response.getTopologyId());
        assertEquals(topologyContextId, response.getTopologyContextId());
    }

    @Test
    public void testTopologyEntityAddition() throws Exception {

        when(identityProvider.getCloneId(eq(testEntity.build()))).thenReturn(11L);
        when(entityStore.constructTopology()).thenReturn(ImmutableMap.of(entityId, testEntity));

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                .addScenarioChange(ScenarioChange.newBuilder()
                    .setTopologyAddition(TopologyAddition.newBuilder()
                        .setEntityId(entityId)))
                .build());

        final TopologyEntityDTO expectedClone = TopologyEntityDTO.newBuilder(testEntity.build())
                .setOid(11L)
                .setDisplayName("test - Clone")
                .build();

        verify(topologyHandler).broadcastTopology(eq(topologyInfo), broadcastCaptor.capture());

        final Set<TopologyEntityDTO> newTopology =
                broadcastCaptor.getValue().collect(Collectors.toSet());

        MatcherAssert.assertThat(newTopology,
                Matchers.containsInAnyOrder(
                        Matchers.equalTo(testEntity.build()), Matchers.equalTo(expectedClone)));
    }

    @Test
    public void testTopologyEntityMultiAddition() throws Exception {
        when(identityProvider.getCloneId(eq(testEntity.build()))).thenReturn(11L, 12L);
        when(entityStore.constructTopology()).thenReturn(ImmutableMap.of(entityId, testEntity));

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                .addScenarioChange(ScenarioChange.newBuilder()
                        .setTopologyAddition(TopologyAddition.newBuilder()
                                .setAdditionCount(2)
                                .setEntityId(entityId)))
                .build());

        final TopologyEntityDTO expectedClone1 = TopologyEntityDTO.newBuilder()
                .setOid(11L)
                .setDisplayName("test - Clone")
                .setEntityType(1)
                .build();
        final TopologyEntityDTO expectedClone2 = TopologyEntityDTO.newBuilder(expectedClone1)
                .setOid(12)
                .build();

        verify(topologyHandler).broadcastTopology(eq(topologyInfo), broadcastCaptor.capture());
        final Set<TopologyEntityDTO> newTopology =
                broadcastCaptor.getValue().collect(Collectors.toSet());

        MatcherAssert.assertThat(newTopology,
                Matchers.containsInAnyOrder(
                        Matchers.equalTo(testEntity.build()), Matchers.equalTo(expectedClone1),
                        Matchers.equalTo(expectedClone2)));

    }

    @Test
    public void testTopologyEntityAdditionMissing() throws Exception {
        when(entityStore.constructTopology()).thenReturn(ImmutableMap.of(entityId, testEntity));

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                .addScenarioChange(ScenarioChange.newBuilder()
                        .setTopologyAddition(TopologyAddition.newBuilder()
                                .setEntityId(entityId + 1)))
                .build());

        verify(topologyHandler).broadcastTopology(eq(topologyInfo), broadcastCaptor.capture());
        final Set<TopologyEntityDTO> newTopology =
                broadcastCaptor.getValue().collect(Collectors.toSet());

        MatcherAssert.assertThat(newTopology,
                Matchers.contains(Matchers.equalTo(testEntity.build())));
    }

    @Test
    public void testTopologyEntityRemoval() throws Exception {
        when(entityStore.constructTopology()).thenReturn(ImmutableMap.of(entityId, testEntity));

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                .addScenarioChange(ScenarioChange.newBuilder()
                    .setTopologyRemoval(TopologyRemoval.newBuilder()
                    .setEntityId(entityId)))
                .build());

        verify(topologyHandler).broadcastTopology(eq(topologyInfo), broadcastCaptor.capture());

        final Set<TopologyEntityDTO> newTopology =
                broadcastCaptor.getValue().collect(Collectors.toSet());
        Assert.assertTrue(newTopology.isEmpty());
    }

    @Test
    public void testTopologyEntityRemovalMissing() throws Exception {
        when(entityStore.constructTopology()).thenReturn(ImmutableMap.of(entityId, testEntity));

        analysisService.startAnalysis(StartAnalysisRequest.newBuilder()
                .setPlanId(planId)
                .addScenarioChange(ScenarioChange.newBuilder()
                        .setTopologyRemoval(TopologyRemoval.newBuilder()
                                .setEntityId(entityId + 1)))
                .build());

        verify(topologyHandler).broadcastTopology(eq(topologyInfo), broadcastCaptor.capture());

        final Set<TopologyEntityDTO> newTopology =
                broadcastCaptor.getValue().collect(Collectors.toSet());
        MatcherAssert.assertThat(newTopology,
                Matchers.contains(Matchers.equalTo(testEntity.build())));
    }
}
