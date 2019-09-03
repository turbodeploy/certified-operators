package com.vmturbo.topology.processor.topology.pipeline;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.entity.EntitiesValidationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupMemberCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.stitching.journal.EmptyStitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal.StitchingJournalContainer;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ChangeAppCommodityKeyOnVMAndAppStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EntityValidationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GraphCreationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PlanScopingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PolicyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PostStitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScanDiscoveredSettingPoliciesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScopeResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingGroupFixupStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyAcquisitionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadTemplatesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadWorkflowsStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Status;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

public class StagesTest {

    final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
            .setEntityType(10)
            .setOid(7L);

    @SuppressWarnings("unchecked")
    final StitchingJournal<TopologyEntity> journal = mock(StitchingJournal.class);

    private GrpcTestServer testServer;

    @Test
    public void testUploadGroupsStage() {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(7L, topologyEntityBuilder(entity));
        final DiscoveredGroupUploader uploader = mock(DiscoveredGroupUploader.class);
        final UploadGroupsStage stage = new UploadGroupsStage(uploader);
        stage.passthrough(topology);
        verify(uploader).uploadDiscoveredGroups(topology);
    }

    @Test
    public void testEmptyPlanScopingStage() throws PipelineStageException, InterruptedException {
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final StitchingJournalContainer container = new StitchingJournalContainer();
        final IStitchingJournal<TopologyEntity> journal = spy(new EmptyStitchingJournal<TopologyEntity>());
        container.setPostStitchingJournal(journal);
        final SearchResolver<TopologyEntity> searchResolver = mock(SearchResolver.class);
        final GroupResolver groupResolver = new GroupResolver(searchResolver);
        final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(1)
                        .setTopologyId(1)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
                        .build();
        final PlanTopologyScopeEditor scopeEditor = mock(PlanTopologyScopeEditor.class);

        when(context.getStitchingJournalContainer()).thenReturn(container);
        when(graph.entities()).thenReturn(Stream.empty());
        when(context.getTopologyInfo()).thenReturn(topologyInfo);

        PlanScope emptyScope = PlanScope.newBuilder().build();
        final PlanScopingStage emptyScopingStage = new PlanScopingStage(scopeEditor, emptyScope , searchResolver,
                                                                   new ArrayList<ScenarioChange>());
        assertTrue(emptyScopingStage.execute(graph).getResult().entities().count() == 0);
    }

    @Test
    public void testCloudPlanScopingStage() throws PipelineStageException, InterruptedException {
        final TopologyInfo cloudTopologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(1)
                        .setTopologyId(1)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
                        .build();
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final StitchingJournalContainer container = new StitchingJournalContainer();
        final PlanTopologyScopeEditor scopeEditor = mock(PlanTopologyScopeEditor.class);
        final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);
        final SearchResolver<TopologyEntity> searchResolver = mock(SearchResolver.class);
        final PlanScope scope = PlanScope.newBuilder().addScopeEntries(PlanScopeEntry
                .newBuilder().setClassName(StringConstants.CLUSTER).setScopeObjectOid(11111)).build();
        final PlanScopingStage cloudScopingStage = spy(new PlanScopingStage(scopeEditor, scope , searchResolver,
                new ArrayList<ScenarioChange>()));
        when(cloudScopingStage.getContext()).thenReturn(context);
        when(context.getStitchingJournalContainer()).thenReturn(container);
        when(context.getTopologyInfo()).thenReturn(cloudTopologyInfo);
        when(scopeEditor.scopeCloudTopology(graph, scope)).thenReturn(graph);
        cloudScopingStage.execute(graph);
        verify(scopeEditor).scopeCloudTopology(graph, scope);
    }

    @Test
    public void testOnpremPlanScopingStage() throws PipelineStageException, InterruptedException {
        final TopologyInfo onpremTopologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(2)
                        .setTopologyId(2)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("CUSTOM").build())
                        .build();
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final PlanTopologyScopeEditor scopeEditor = mock(PlanTopologyScopeEditor.class);
        final SearchResolver<TopologyEntity> searchResolver = mock(SearchResolver.class);
        final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);
        final PlanScope scope = PlanScope.newBuilder().addScopeEntries(PlanScopeEntry
                .newBuilder().setClassName(StringConstants.CLUSTER).setScopeObjectOid(11111)).build();
        List<ScenarioChange> changes = new ArrayList<ScenarioChange>();
        final PlanScopingStage onpremScopingStage = spy(new PlanScopingStage(scopeEditor, scope , searchResolver, changes));
        TopologyGraph<TopologyEntity> result = mock(TopologyGraph.class);
        when(onpremScopingStage.getContext()).thenReturn(context);
        when(context.getTopologyInfo()).thenReturn(onpremTopologyInfo);
        when(scopeEditor.scopeOnPremTopology(eq(onpremTopologyInfo), eq(graph), eq(scope), any(), eq(changes))).thenReturn(result);
        when(result.size()).thenReturn(0);
        when(graph.size()).thenReturn(0);
        onpremScopingStage.execute(graph);
        verify(scopeEditor).scopeOnPremTopology(eq(onpremTopologyInfo), eq(graph), eq(scope), any(), eq(changes));
    }

    @Test
    public void testUploadWorkflowsStage() {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(7L, topologyEntityBuilder(entity));
        final DiscoveredWorkflowUploader uploader = mock(DiscoveredWorkflowUploader.class);
        final UploadWorkflowsStage stage = new UploadWorkflowsStage(uploader);
        stage.passthrough(topology);
        verify(uploader).uploadDiscoveredWorkflows();
    }

    @Test
    public void testUploadTemplatesStage() throws Exception {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(7L, topologyEntityBuilder(entity));
        final DiscoveredTemplateDeploymentProfileNotifier uploader =
                mock(DiscoveredTemplateDeploymentProfileNotifier.class);
        final UploadTemplatesStage stage = new UploadTemplatesStage(uploader);
        stage.passthrough(topology);
        verify(uploader).sendTemplateDeploymentProfileData();
    }

    @Test
    public void testUploadTemplatesStageException() throws Exception {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(7L, topologyEntityBuilder(entity));
        final DiscoveredTemplateDeploymentProfileNotifier uploader =
                mock(DiscoveredTemplateDeploymentProfileNotifier.class);
        doThrow(CommunicationException.class).when(uploader).sendTemplateDeploymentProfileData();
        final UploadTemplatesStage stage = new UploadTemplatesStage(uploader);
        assertThat(stage.passthrough(topology).getType(), is(TopologyPipeline.Status.Type.FAILED));
    }

    @Test
    public void testStitchingStage() {
        final StitchingManager stitchingManager = mock(StitchingManager.class);
        final EntityStore entityStore = mock(EntityStore.class);
        final StitchingContext stitchingContext = mock(StitchingContext.class);
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final StitchingJournalFactory journalFactory = mock(StitchingJournalFactory.class);
        final StitchingJournalContainer container = new StitchingJournalContainer();
        final IStitchingJournal<StitchingEntity> journal = spy(new EmptyStitchingJournal<>());
        final TopologyStitchingGraph graph = mock(TopologyStitchingGraph.class);

        when(journalFactory.stitchingJournal(eq(stitchingContext))).thenReturn(journal);
        when(entityStore.constructStitchingContext()).thenReturn(stitchingContext);
        when(stitchingManager.stitch(eq(stitchingContext), eq(journal))).thenReturn(stitchingContext);
        when(stitchingContext.constructTopology()).thenReturn(Collections.emptyMap());
        when(context.getStitchingJournalContainer()).thenReturn(container);
        when(stitchingContext.entityTypeCounts()).thenReturn(Collections.emptyMap());
        when(journal.shouldDumpTopologyBeforePreStitching()).thenReturn(true);
        when(stitchingContext.getStitchingGraph()).thenReturn(graph);
        when(graph.entities()).thenReturn(Stream.empty());

        final StitchingStage stitchingStage = new StitchingStage(stitchingManager, journalFactory);
        stitchingStage.setContext(context);
        assertThat(stitchingStage.execute(entityStore).getResult().constructTopology(), is(Collections.emptyMap()));
        assertTrue(container.getMainStitchingJournal().isPresent());
        assertFalse(container.getPostStitchingJournal().isPresent());

        verify(journal).dumpTopology(any(Stream.class));
    }

    @Test
    public void testStitchingGroupFixup() throws PipelineStageException {
        final StitchingGroupFixer stitchingGroupFixer = mock(StitchingGroupFixer.class);
        final DiscoveredGroupUploader uploader = mock(DiscoveredGroupUploader.class);
        final DiscoveredGroupMemberCache memberCache = mock(DiscoveredGroupMemberCache.class);
        final StitchingContext stitchingContext = mock(StitchingContext.class);
        final TopologyStitchingGraph stitchingGraph = mock(TopologyStitchingGraph.class);

        when(uploader.buildMemberCache()).thenReturn(memberCache);
        when(stitchingContext.getStitchingGraph()).thenReturn(stitchingGraph);

        final StitchingGroupFixupStage fixupStage = new StitchingGroupFixupStage(stitchingGroupFixer, uploader);
        fixupStage.passthrough(stitchingContext);
        verify(stitchingGroupFixer).fixupGroups(stitchingGraph, memberCache);
    }

    @Test
    public void testScanDiscoveredSettingPoliciesStage() {
        final DiscoveredGroupUploader uploader = mock(DiscoveredGroupUploader.class);
        final DiscoveredSettingPolicyScanner scanner = mock(DiscoveredSettingPolicyScanner.class);
        final StitchingContext stitchingContext = mock(StitchingContext.class);


        final ScanDiscoveredSettingPoliciesStage scannerStage =
                new ScanDiscoveredSettingPoliciesStage(scanner, uploader);
        scannerStage.passthrough(stitchingContext);
        verify(scanner).scanForDiscoveredSettingPolicies(eq(stitchingContext), eq(uploader));
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
        Map<Long, TopologyEntity.Builder> ret = acquisitionStage.execute(1L).getResult();
        assertTrue(ret.containsKey(7L));
        assertThat(ret.get(7L).getOid(), is(7L));
        assertThat(ret.get(7L).getEntityType(), is(10));
    }

    @Test
    public void testEditStage() throws PipelineStageException {
        final TopologyEditor topologyEditor = mock(TopologyEditor.class);
        final List<ScenarioChange> changes = Collections.emptyList();
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(1)
                .setTopologyId(1)
                .setCreationTime(System.currentTimeMillis())
                .setTopologyType(TopologyType.PLAN)
                .build();
        SearchResolver<TopologyEntity> searchResolver = mock(SearchResolver.class);
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(topologyInfo);
        final TopologyEditStage stage =
                new TopologyEditStage(topologyEditor, searchResolver, changes);
        stage.setContext(context);
        stage.execute(Collections.emptyMap());
        verify(topologyEditor).editTopology(eq(Collections.emptyMap()),
                eq(Collections.emptyList()), any(), any(GroupResolver.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPostStitchingStage() throws PipelineStageException {
        final StitchingManager stitchingManager = mock(StitchingManager.class);
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final PostStitchingStage postStitchingStage = new PostStitchingStage(stitchingManager);
        final GraphWithSettings graphWithSettings = mock(GraphWithSettings.class);
        final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);

        final StitchingJournalContainer container = new StitchingJournalContainer();

        final IStitchingJournal<StitchingEntity> mainJournal = mock(IStitchingJournal.class);
        final IStitchingJournal<TopologyEntity> postStitchingJournal = mock(IStitchingJournal.class);
        when(mainJournal.getJournalOptions()).thenReturn(JournalOptions.getDefaultInstance());
        when(mainJournal.<TopologyEntity>childJournal(any())).thenReturn(postStitchingJournal);
        container.setMainStitchingJournal(mainJournal);
        when(context.getStitchingJournalContainer()).thenReturn(container);
        when(postStitchingJournal.shouldDumpTopologyAfterPostStitching()).thenReturn(true);
        when(graphWithSettings.getTopologyGraph()).thenReturn(graph);
        when(graph.entities()).thenReturn(Stream.empty());

        postStitchingStage.setContext(context);
        postStitchingStage.execute(graphWithSettings);

        verify(stitchingManager).postStitch(eq(graphWithSettings), eq(postStitchingJournal));
        verify(postStitchingJournal).dumpTopology(any(Stream.class));
    }

    @Test
    public void testGraphCreationStage() {
        final Map<Long, TopologyEntity.Builder> topology = ImmutableMap.of(7L, topologyEntityBuilder(entity));

        final GraphCreationStage stage = new GraphCreationStage();
        final TopologyGraph<TopologyEntity> topologyGraph = stage.execute(topology).getResult();
        assertThat(topologyGraph.size(), is(1));
        assertThat(topologyGraph.getEntity(7L).get().getTopologyEntityDtoBuilder(),
                is(entity));
    }

    @Test
    public void testPolicyStage() throws PipelineStageException {
        final PolicyManager policyManager = mock(PolicyManager.class);

        final PolicyStage policyStage = new PolicyStage(policyManager);

        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        final GroupResolver groupResolver = mock(GroupResolver.class);
        when(context.getGroupResolver()).thenReturn(groupResolver);

        final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);
        policyStage.setContext(context);

        final PolicyApplicator.Results results = mock(PolicyApplicator.Results.class);
        when(results.errors()).thenReturn(Collections.emptyMap());
        when(results.appliedCounts()).thenReturn(Collections.emptyMap());
        when(results.addedCommodityCounts()).thenReturn(Collections.emptyMap());

        when(policyManager.applyPolicies(topologyGraph, groupResolver, Collections.emptyList()))
            .thenReturn(results);

        policyStage.execute(topologyGraph);

        verify(policyManager).applyPolicies(eq(topologyGraph), eq(groupResolver), eq(Collections.emptyList()));
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


        final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);

        final GraphWithSettings graphWithSettings = mock(GraphWithSettings.class);

        when(entitySettingsResolver.resolveSettings(eq(groupResolver), eq(topologyGraph), any(), any()))
            .thenReturn(graphWithSettings);

        stage.setContext(context);
        stage.execute(topologyGraph);

        verify(entitySettingsResolver).resolveSettings(eq(groupResolver), eq(topologyGraph), any(), any());
    }

    @Test
    public void testBroadcastStage() throws Exception {
        final TopoBroadcastManager broadcastManager1 = mock(TopoBroadcastManager.class);
        final TopoBroadcastManager broadcastManager2 = mock(TopoBroadcastManager.class);
        final BroadcastStage stage = new BroadcastStage(Arrays.asList(broadcastManager1,
                broadcastManager2));

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyContextId(1L)
                .setTopologyId(2L)
                .build();
        final StitchingJournalContainer container = new StitchingJournalContainer();
        @SuppressWarnings("unchecked")
        final IStitchingJournal<TopologyEntity> postStitchingJournal = mock(IStitchingJournal.class);
        container.setPostStitchingJournal(postStitchingJournal);
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(topologyInfo);
        when(context.getStitchingJournalContainer()).thenReturn(container);
        stage.setContext(context);

        final TopologyBroadcast broadcast1 = mock(TopologyBroadcast.class);
        final TopologyBroadcast broadcast2 = mock(TopologyBroadcast.class);
        when(broadcast1.finish()).thenReturn(1L);
        when(broadcast2.finish()).thenReturn(1L);

        when(broadcastManager1.broadcastLiveTopology(eq(topologyInfo)))
                .thenReturn(broadcast1);
        when(broadcastManager2.broadcastLiveTopology(eq(topologyInfo)))
                .thenReturn(broadcast2);

        final TopologyBroadcastInfo broadcastInfo = stage.execute(createTopologyGraph()).getResult();
        assertThat(broadcastInfo.getEntityCount(), is(1L));
        assertThat(broadcastInfo.getTopologyContextId(), is(1L));
        assertThat(broadcastInfo.getTopologyId(), is(2L));

        verify(broadcast1).append(any());
        verify(broadcast1).append(eq(entity.build()));

        verify(broadcast2).append(any());
        verify(broadcast2).append(eq(entity.build()));
        verify(postStitchingJournal).recordTopologyInfoAndMetrics(any(), any());
        verify(postStitchingJournal).flushRecorders();
    }

    @Test
    public void testPlanBroadcastStage() throws CommunicationException, InterruptedException, PipelineStageException {
        final TopoBroadcastManager broadcastManager = mock(TopoBroadcastManager.class);
        final BroadcastStage stage = new BroadcastStage(Collections.singletonList(broadcastManager));

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(2L)
                .setTopologyContextId(1L)
                .setTopologyType(TopologyType.PLAN)
                .build();
        final TopologyPipelineContext context = mock(TopologyPipelineContext.class);
        when(context.getTopologyInfo()).thenReturn(topologyInfo);
        when(context.getStitchingJournalContainer()).thenReturn(new StitchingJournalContainer());
        stage.setContext(context);

        final TopologyBroadcast broadcast = mock(TopologyBroadcast.class);
        when(broadcast.getTopologyContextId()).thenReturn(1L);
        when(broadcast.getTopologyId()).thenReturn(2L);
        when(broadcast.finish()).thenReturn(1L);

        when(broadcastManager.broadcastUserPlanTopology(eq(topologyInfo)))
                .thenReturn(broadcast);

        final TopologyBroadcastInfo broadcastInfo = stage.execute(createTopologyGraph()).getResult();
        assertThat(broadcastInfo.getEntityCount(), is(1L));
        assertThat(broadcastInfo.getTopologyContextId(), is(1L));
        assertThat(broadcastInfo.getTopologyId(), is(2L));

        verify(broadcast).append(any());
        verify(broadcast).append(eq(entity.build()));
    }

    @Test
    public void testEntityValidationStage() throws Exception {
        final EntityValidator entityValidator = mock(EntityValidator.class);
        final GraphWithSettings graphWithSettings = mock(GraphWithSettings.class);
        final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);
        when(graphWithSettings.getTopologyGraph()).thenReturn(topologyGraph);
        when(topologyGraph.entities()).thenReturn(Stream.empty());

        final EntityValidationStage entityValidationStage = new EntityValidationStage(entityValidator);
        entityValidationStage.passthrough(graphWithSettings);
        verify(entityValidator).validateTopologyEntities(any());
    }

    @Test
    public void testEntityValidationStageFailure() throws Exception {
        final EntityValidator entityValidator = mock(EntityValidator.class);
        final GraphWithSettings graphWithSettings = mock(GraphWithSettings.class);
        final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);
        when(graphWithSettings.getTopologyGraph()).thenReturn(topologyGraph);
        when(topologyGraph.entities()).thenReturn(Stream.empty());
        doThrow(new EntitiesValidationException(Collections.emptyList()))
                .when(entityValidator).validateTopologyEntities(any());

        final EntityValidationStage entityValidationStage = new EntityValidationStage(entityValidator);
        try {
            entityValidationStage.passthrough(graphWithSettings);
            fail();
        } catch (PipelineStageException e) {
            //expected
        }
        verify(entityValidator).validateTopologyEntities(any());
    }

    @Test
    public void testChangeAppCommodityKeyOnVMAndAppStage() throws Exception {
        final ApplicationCommodityKeyChanger applicationCommodityKeyChanger = mock(ApplicationCommodityKeyChanger.class);
        final ChangeAppCommodityKeyOnVMAndAppStage changeAppCommodityKeyOnVMAndAppStage = new ChangeAppCommodityKeyOnVMAndAppStage(applicationCommodityKeyChanger);
        final TopologyGraph<TopologyEntity> topologyGraph = mock(TopologyGraph.class);

        changeAppCommodityKeyOnVMAndAppStage.passthrough(topologyGraph);
        verify(applicationCommodityKeyChanger).execute(any());
    }

    /**
     * Tests Scope resolution stage for the empty scope. Result status of the stage is succeeded.
     *
     * @throws IOException if there was error during test server start.
     * @throws PipelineStageException if there was error during stage execution.
     */
    @Test
    public void testScopeResolutionStageWithEmptyScope() throws IOException, PipelineStageException {
        final GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);
        testServer = GrpcTestServer.newServer(groupServiceMole);
        testServer.start();
        final GroupServiceBlockingStub groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        final PlanScope emptyScope = PlanScope.newBuilder().build();
        final ScopeResolutionStage stage = new ScopeResolutionStage(groupService, emptyScope);
        final Status status = stage.passthrough(createTopologyGraph());
        testServer.close();
        Assert.assertEquals(Status.Type.SUCCEEDED, status.getType());
        Assert.assertEquals("No scope to apply.", status.getMessage());
    }

    /**
     * Tests Scope resolution stage for the region scope.
     *
     * @throws IOException if there was error during test server start.
     * @throws PipelineStageException if there was error during stage execution.
     */
    @Test
    public void testScopeResolutionStage() throws IOException, PipelineStageException {
        final GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);
        testServer = GrpcTestServer.newServer(groupServiceMole);
        testServer.start();
        final GroupServiceBlockingStub groupService = GroupServiceGrpc
                        .newBlockingStub(testServer.getChannel());
        final PlanScope scope = PlanScope.newBuilder()
                        .addScopeEntries(
                                         PlanScopeEntry.newBuilder()
                                                         .setClassName(StringConstants.REGION)
                                                         .setScopeObjectOid(11111))
                        .addScopeEntries(
                                         PlanScopeEntry.newBuilder()
                                                         .setClassName(StringConstants.BUSINESS_ACCOUNT)
                                                         .setScopeObjectOid(22222))
                        .build();
        final ScopeResolutionStage stage = new ScopeResolutionStage(groupService, scope);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                        .setTopologyContextId(1)
                        .setTopologyId(1)
                        .setCreationTime(System.currentTimeMillis())
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanType("OPTIMIZE_CLOUD").build())
                        .build();
        final GroupResolver groupResolver = mock(GroupResolver.class);
        final TopologyPipelineContext context = new TopologyPipelineContext(groupResolver, topologyInfo);
        stage.setContext(context);
        final Status status = stage.passthrough(createTopologyGraph());
        testServer.close();
        Assert.assertEquals(Status.Type.SUCCEEDED, status.getType());
        final TopologyInfo topoResult = context.getTopologyInfo();
        Assert.assertEquals(2, topoResult.getScopeSeedOidsCount());
        Assert.assertEquals(11111, topoResult.getScopeSeedOids(0));
        Assert.assertEquals(22222, topoResult.getScopeSeedOids(1));
    }

    private TopologyGraph<TopologyEntity> createTopologyGraph() {
        final TopologyGraph<TopologyEntity> graph = mock(TopologyGraph.class);
        final TopologyEntity entity = mock(TopologyEntity.class);
        when(entity.getTopologyEntityDtoBuilder()).thenReturn(this.entity);
        when(graph.entities()).thenReturn(Stream.of(entity));
        return graph;
    }

}