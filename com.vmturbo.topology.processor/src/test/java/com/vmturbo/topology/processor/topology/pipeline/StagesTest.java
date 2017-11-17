package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GraphCreationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PolicyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyAcquisitionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadTemplatesStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

public class StagesTest {

    final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
            .setEntityType(10)
            .setOid(7L);

    @Test
    public void testUploadGroupsStage() {
        final EntityStore entityStore = mock(EntityStore.class);
        final DiscoveredGroupUploader uploader = mock(DiscoveredGroupUploader.class);
        final UploadGroupsStage stage = new UploadGroupsStage(uploader);
        stage.passthrough(entityStore);
        verify(uploader).processQueuedGroups();
    }

    @Test
    public void testUploadTemplatesStage() throws Exception {
        final EntityStore entityStore = mock(EntityStore.class);
        final DiscoveredTemplateDeploymentProfileNotifier uploader =
                mock(DiscoveredTemplateDeploymentProfileNotifier.class);
        final UploadTemplatesStage stage = new UploadTemplatesStage(uploader);
        stage.passthrough(entityStore);
        verify(uploader).sendTemplateDeploymentProfileData();
    }

    @Test(expected = PipelineStageException.class)
    public void testUploadTemplatesStageException() throws Exception {
        final EntityStore entityStore = mock(EntityStore.class);
        final DiscoveredTemplateDeploymentProfileNotifier uploader =
                mock(DiscoveredTemplateDeploymentProfileNotifier.class);
        doThrow(CommunicationException.class).when(uploader).sendTemplateDeploymentProfileData();
        final UploadTemplatesStage stage = new UploadTemplatesStage(uploader);
        stage.passthrough(entityStore);
    }

    @Test
    public void testStichingStage() {
        final StitchingManager stitchingManager = mock(StitchingManager.class);
        final EntityStore entityStore = mock(EntityStore.class);
        final StitchingContext stitchingContext = mock(StitchingContext.class);

        when(stitchingManager.stitch(eq(entityStore))).thenReturn(stitchingContext);
        when(stitchingContext.constructTopology()).thenReturn(Collections.emptyMap());

        final StitchingStage stitchingStage = new StitchingStage(stitchingManager);
        assertThat(stitchingStage.execute(entityStore), is(Collections.emptyMap()));
    }

    @Test
    public void testAcquisitionStage() {
        final RetrieveTopologyResponse response = RetrieveTopologyResponse.newBuilder()
                .addEntities(entity)
                .build();
        final RepositoryClient repositoryClient = mock(RepositoryClient.class);
        when(repositoryClient.retrieveTopology(eq(1L)))
            .thenReturn(Collections.singleton(response).iterator());

        final TopologyAcquisitionStage acquisitionStage =
                new TopologyAcquisitionStage(repositoryClient);
        Map<Long, Builder> ret = acquisitionStage.execute(1L);
        assertTrue(ret.containsKey(7L));
        assertThat(ret.get(7L).getOid(), is(7L));
        assertThat(ret.get(7L).getEntityType(), is(10));
    }

    @Test
    public void testEditStage() throws PipelineStageException {
        final TopologyEditor topologyEditor = mock(TopologyEditor.class);
        final List<ScenarioChange> changes = Collections.emptyList();
        final TopologyEditStage stage = new TopologyEditStage(topologyEditor, changes);
        stage.execute(Collections.emptyMap());
        verify(topologyEditor).editTopology(eq(Collections.emptyMap()), eq(Collections.emptyList()));
    }

    @Test
    public void testGraphCreationStage() {
        final Map<Long, TopologyEntityDTO.Builder> topology = ImmutableMap.of(7L, entity);

        final GraphCreationStage stage = new GraphCreationStage();
        final TopologyGraph topologyGraph = stage.execute(topology);
        assertThat(topologyGraph.vertexCount(), is(1));
        assertThat(topologyGraph.getVertex(7L).get().getTopologyEntityDtoBuilder(),
                is(entity));
    }

    @Test
    public void testPolicyStage() throws PipelineStageException {
        final PolicyManager policyManager = mock(PolicyManager.class);

        final PolicyStage policyStage = new PolicyStage(policyManager);

        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final GroupResolver groupResolver = mock(GroupResolver.class);
        when(context.getGroupResolver()).thenReturn(groupResolver);

        final TopologyGraph topologyGraph = mock(TopologyGraph.class);
        policyStage.setContext(context);

        policyStage.execute(topologyGraph);

        verify(policyManager).applyPolicies(eq(topologyGraph), eq(groupResolver));
    }

    @Test
    public void testLiveSettingsResolutionStage() throws PipelineStageException {
        final EntitySettingsResolver entitySettingsResolver = mock(EntitySettingsResolver.class);
        final SettingsResolutionStage stage = SettingsResolutionStage.live(entitySettingsResolver);

        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final GroupResolver groupResolver = mock(GroupResolver.class);

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(7L)
                .setTopologyId(10L)
                .build();

        when(context.getGroupResolver()).thenReturn(groupResolver);
        when(context.getTopologyInfo()).thenReturn(topologyInfo);

        final TopologyGraph topologyGraph = mock(TopologyGraph.class);
        stage.setContext(context);
        stage.execute(topologyGraph);

        verify(entitySettingsResolver).resolveSettings(eq(groupResolver), eq(topologyGraph),
                eq(Collections.emptyMap()));
    }

    @Test
    public void testBroadcastStage() throws Exception {

        final TopoBroadcastManager broadcastManager = mock(TopoBroadcastManager.class);
        final BroadcastStage stage = new BroadcastStage(broadcastManager);

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .build();
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(topologyInfo);
        stage.setContext(context);

        final TopologyBroadcast broadcast = mock(TopologyBroadcast.class);
        when(broadcast.getTopologyContextId()).thenReturn(1L);
        when(broadcast.getTopologyId()).thenReturn(2L);
        when(broadcast.finish()).thenReturn(1L);

        when(broadcastManager.broadcastLiveTopology(eq(topologyInfo)))
            .thenReturn(broadcast);

        final TopologyBroadcastInfo broadcastInfo = stage.execute(createTopologyGraph());
        assertThat(broadcastInfo.getEntityCount(), is(1L));
        assertThat(broadcastInfo.getTopologyContextId(), is(1L));
        assertThat(broadcastInfo.getTopologyId(), is(2L));

        verify(broadcast).append(any());
        verify(broadcast).append(eq(entity.build()));
    }

    @Test
    public void testPlanBroadcastStage() throws CommunicationException, InterruptedException, PipelineStageException {
        final TopoBroadcastManager broadcastManager = mock(TopoBroadcastManager.class);
        final BroadcastStage stage = new BroadcastStage(broadcastManager);

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN)
                .build();
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(topologyInfo);
        stage.setContext(context);

        final TopologyBroadcast broadcast = mock(TopologyBroadcast.class);
        when(broadcast.getTopologyContextId()).thenReturn(1L);
        when(broadcast.getTopologyId()).thenReturn(2L);
        when(broadcast.finish()).thenReturn(1L);

        when(broadcastManager.broadcastUserPlanTopology(eq(topologyInfo)))
                .thenReturn(broadcast);

        final TopologyBroadcastInfo broadcastInfo = stage.execute(createTopologyGraph());
        assertThat(broadcastInfo.getEntityCount(), is(1L));
        assertThat(broadcastInfo.getTopologyContextId(), is(1L));
        assertThat(broadcastInfo.getTopologyId(), is(2L));

        verify(broadcast).append(any());
        verify(broadcast).append(eq(entity.build()));
    }

    private TopologyGraph createTopologyGraph() {
        final TopologyGraph graph = mock(TopologyGraph.class);
        final Vertex vertex = mock(Vertex.class);
        when(vertex.getTopologyEntityDtoBuilder()).thenReturn(entity);
        when(graph.vertices()).thenReturn(Stream.of(vertex));
        return graph;
    }
}
