package com.vmturbo.topology.processor.topology.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.CommoditiesEditor;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ApplyClusterCommodityStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CacheWritingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CachingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ChangeAppCommodityKeyOnVMAndAppStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CommoditiesEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ControllableStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EntityValidationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EnvironmentTypeStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ExtractTopologyGraphStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GraphCreationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoricalUtilizationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoryAggregationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.IgnoreConstraintsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PlanScopingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PolicyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PostStitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ProbeActionCapabilitiesApplicatorStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ReservationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScanDiscoveredSettingPoliciesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScopeResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsApplicationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsUploadStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingGroupFixupStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SupplyChainValidationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyAcquisitionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadCloudCostDataStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadTemplatesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadWorkflowsStage;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * A factory class for properly configured {@link TopologyPipeline} objects.
 * <p>
 * Users should not instantiate {@link TopologyPipeline}s themselves. Instead, they should
 * use the appropriately configured pipelines provided by this factory - e.g.
 * {@link TopologyPipelineFactory#liveTopology(TopologyInfo, List, StitchingJournalFactory)}.
 */
public class TopologyPipelineFactory {

    private final TopoBroadcastManager topoBroadcastManager;

    private final PolicyManager policyManager;

    private final StitchingManager stitchingManager;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final DiscoveredWorkflowUploader discoveredWorkflowUploader;

    private final DiscoveredCloudCostUploader discoveredCloudCostUploader;

    private final EntitySettingsApplicator settingsApplicator;

    private final EntitySettingsResolver entitySettingsResolver;

    private final EnvironmentTypeInjector environmentTypeInjector;

    private final TopologyEditor topologyEditor;

    private final RepositoryClient repositoryClient;

    private final SearchResolver<TopologyEntity> searchResolver;

    private final GroupServiceBlockingStub groupServiceClient;

    private final ReservationManager reservationManager;

    private final DiscoveredSettingPolicyScanner discoveredSettingPolicyScanner;

    private final StitchingGroupFixer stitchingGroupFixer;

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

    private final ApplicationCommodityKeyChanger applicationCommodityKeyChanger;

    private final EntityValidator entityValidator;

    private final SupplyChainValidator supplyChainValidator;

    private final ControllableManager controllableManager;

    private final CommoditiesEditor commoditiesEditor;

    private final PlanTopologyScopeEditor planTopologyScopeEditor;

    private final HistoricalEditor historicalEditor;

    private final MatrixInterface matrix;

    private final CachedTopology constructTopologyStageCache = new CachedTopology();

    private final ProbeActionCapabilitiesApplicatorEditor applicatorEditor;

    private final HistoryAggregator historyAggregator;

    public TopologyPipelineFactory(@Nonnull final TopoBroadcastManager topoBroadcastManager,
                                   @Nonnull final PolicyManager policyManager,
                                   @Nonnull final StitchingManager stitchingManager,
                                   @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
                                   @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
                                   @Nonnull final DiscoveredWorkflowUploader discoveredWorkflowUploader,
                                   @Nonnull final DiscoveredCloudCostUploader cloudCostUploader,
                                   @Nonnull final EntitySettingsResolver entitySettingsResolver,
                                   @Nonnull final EntitySettingsApplicator settingsApplicator,
                                   @Nonnull final EnvironmentTypeInjector environmentTypeInjector,
                                   @Nonnull final TopologyEditor topologyEditor,
                                   @Nonnull final RepositoryClient repositoryClient,
                                   @Nonnull final SearchResolver<TopologyEntity> searchResolver,
                                   @Nonnull final GroupServiceBlockingStub groupServiceClient,
                                   @Nonnull final ReservationManager reservationManager,
                                   @Nonnull final DiscoveredSettingPolicyScanner discoveredSettingPolicyScanner,
                                   @Nonnull final StitchingGroupFixer stitchingGroupFixer,
                                   @Nonnull final EntityValidator entityValidator,
                                   @Nonnull final SupplyChainValidator supplyChainValidator,
                                   @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache,
                                   @Nonnull final ApplicationCommodityKeyChanger applicationCommodityKeyChanger,
                                   @Nonnull final ControllableManager controllableManager,
                                   @Nonnull final CommoditiesEditor commoditiesEditor,
                                   @Nonnull final PlanTopologyScopeEditor planTopologyScopeEditor,
                                   @Nonnull final HistoricalEditor historicalEditor,
                                   @Nonnull final MatrixInterface matrix,
                                   @Nonnull final ProbeActionCapabilitiesApplicatorEditor applicatorEditor,
                                   @Nonnull HistoryAggregator historyAggregationStage) {
        this.topoBroadcastManager = topoBroadcastManager;
        this.policyManager = policyManager;
        this.stitchingManager = stitchingManager;
        this.discoveredTemplateDeploymentProfileNotifier = discoveredTemplateDeploymentProfileNotifier;
        this.discoveredGroupUploader = discoveredGroupUploader;
        this.discoveredWorkflowUploader = discoveredWorkflowUploader;
        this.discoveredCloudCostUploader = cloudCostUploader;
        this.settingsApplicator = Objects.requireNonNull(settingsApplicator);
        this.entitySettingsResolver = entitySettingsResolver;
        this.environmentTypeInjector = Objects.requireNonNull(environmentTypeInjector);
        this.topologyEditor = Objects.requireNonNull(topologyEditor);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.reservationManager = Objects.requireNonNull(reservationManager);
        this.discoveredSettingPolicyScanner = Objects.requireNonNull(discoveredSettingPolicyScanner);
        this.stitchingGroupFixer = Objects.requireNonNull(stitchingGroupFixer);
        this.discoveredClusterConstraintCache = Objects.requireNonNull(discoveredClusterConstraintCache);
        this.applicationCommodityKeyChanger = Objects.requireNonNull(applicationCommodityKeyChanger);
        this.entityValidator = Objects.requireNonNull(entityValidator);
        this.supplyChainValidator = Objects.requireNonNull(supplyChainValidator);
        this.controllableManager = Objects.requireNonNull(controllableManager);
        this.commoditiesEditor = Objects.requireNonNull(commoditiesEditor);
        this.planTopologyScopeEditor = planTopologyScopeEditor;
        this.historicalEditor = Objects.requireNonNull(historicalEditor);
        this.matrix = Objects.requireNonNull(matrix);
        this.applicatorEditor = Objects.requireNonNull(applicatorEditor);
        this.historyAggregator = Objects.requireNonNull(historyAggregationStage);
    }

    /**
     * Create a pipeline that constructs and broadcasts the most up-to-date live topology.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @param additionalBroadcastManagers Broadcast managers in addition to the base one used to broadcast
     *                      the topology. All broadcast managers in the list along with
     *                      the broadcast manager passed in at class construction are used
     *                      to broadcast the topology.
     *                      Inject additional managers to also send the topology to other
     *                      listeners beyond the expected ones (ie to also send it to gRPC
     *                      clients in addition to the ones listening on the base broadcastManager).
     * @param journalFactory The journal factory to be used to create a journal to track changes made
     *                       during stitching.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    public TopologyPipeline<EntityStore, TopologyBroadcastInfo> liveTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<TopoBroadcastManager> additionalBroadcastManagers,
            @Nonnull final StitchingJournalFactory journalFactory) {
        final GroupResolver groupResolver = new GroupResolver(searchResolver, groupServiceClient);
        final TopologyPipelineContext context =
            new TopologyPipelineContext(groupResolver, topologyInfo);
        final List<TopoBroadcastManager> managers = new ArrayList<>(additionalBroadcastManagers.size() + 1);
        managers.add(topoBroadcastManager);
        managers.addAll(additionalBroadcastManagers);

        return TopologyPipeline.<EntityStore, TopologyBroadcastInfo>newBuilder(context)
                .addStage(new StitchingStage(stitchingManager, journalFactory))
                .addStage(new Stages.FlowGenerationStage(matrix))
                .addStage(new StitchingGroupFixupStage(stitchingGroupFixer, discoveredGroupUploader))
                .addStage(new UploadCloudCostDataStage(discoveredCloudCostUploader))
                .addStage(new ScanDiscoveredSettingPoliciesStage(discoveredSettingPolicyScanner,
                    discoveredGroupUploader))
                .addStage(new CacheWritingConstructTopologyFromStitchingContextStage(constructTopologyStageCache))
                .addStage(new UploadGroupsStage(discoveredGroupUploader))
                .addStage(new UploadWorkflowsStage(discoveredWorkflowUploader))
                .addStage(new UploadTemplatesStage(discoveredTemplateDeploymentProfileNotifier))
                .addStage(new ReservationStage(reservationManager))
                .addStage(new ControllableStage(controllableManager))
                .addStage(new GraphCreationStage())
                .addStage(new ApplyClusterCommodityStage(discoveredClusterConstraintCache))
                .addStage(new ChangeAppCommodityKeyOnVMAndAppStage(applicationCommodityKeyChanger))
                .addStage(new EnvironmentTypeStage(environmentTypeInjector))
                .addStage(new PolicyStage(policyManager))
                .addStage(SettingsResolutionStage.live(entitySettingsResolver))
                .addStage(new SettingsUploadStage(entitySettingsResolver))
                .addStage(new SettingsApplicationStage(settingsApplicator))
                .addStage(new Stages.MatrixUpdateStage(matrix))
                .addStage(new PostStitchingStage(stitchingManager))
                .addStage(new EntityValidationStage(entityValidator))
                .addStage(new SupplyChainValidationStage(supplyChainValidator))
                .addStage(new HistoryAggregationStage(historyAggregator, null, topologyInfo, null))
                .addStage(new ExtractTopologyGraphStage())
                .addStage(new HistoricalUtilizationStage(historicalEditor))
                .addStage(new ProbeActionCapabilitiesApplicatorStage(applicatorEditor))
                .addStage(new BroadcastStage(managers))
                .build();
    }

    /**
     * Create a pipeline that constructs and broadcasts the most up-to-date live topology AND
     * applies a set of changes to it.
     *
     * TODO: Note that because we don't upload discovered groups/policies during the
     * TODO: planOverLive pipeline, we may have inconsistencies between the entities and
     * TODO: discovered groups/policies. For example, suppose we broadcast 5 minutes before
     * TODO: a planOverLive request and a discovery containing different discovered groups/policies
     * TODO: completed 2 minutes before, the discovered groups/policies in the group component
     * TODO: will reflect the 5 minute old snapshot while the entities that are fed into the
     * TODO: pipeline will be from the 2 minute old snapshot.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @param changes The list of changes to apply.
     * @param journalFactory The journal factory to be used to create a journal to track changes made
     *                       during stitching.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    public TopologyPipeline<EntityStore, TopologyBroadcastInfo> planOverLiveTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<ScenarioChange> changes,
            @Nullable final PlanScope scope,
            @Nonnull final StitchingJournalFactory journalFactory) {
        final TopologyPipelineContext context =
                new TopologyPipelineContext(new GroupResolver(searchResolver, groupServiceClient), topologyInfo);
        final TopologyPipeline.Builder topoPipelineBuilder =
            TopologyPipeline.<EntityStore, TopologyBroadcastInfo>newBuilder(context);
        // if the constructed topology is already in the cache from the realtime topology, just
        // add the stage that will read it from the cache, otherwise add the stitching stage
        // and the construct topology stage so we can build the topology
        if (constructTopologyStageCache.isEmpty()) {
            topoPipelineBuilder.addStage(new StitchingStage(stitchingManager, journalFactory))
                .addStage(new ConstructTopologyFromStitchingContextStage());
        } else {
            topoPipelineBuilder.addStage(
                new CachingConstructTopologyFromStitchingContextStage(constructTopologyStageCache));
        }
        return topoPipelineBuilder
                .addStage(new ReservationStage(reservationManager))
                // TODO: Move the ToplogyEditStage after the GraphCreationStage
                // That way the editstage can work on the graph instead of a
                // separate structure.
                .addStage(new TopologyEditStage(topologyEditor, searchResolver, changes, groupServiceClient))
                .addStage(new GraphCreationStage())
                .addStage(new ApplyClusterCommodityStage(discoveredClusterConstraintCache))
                .addStage(new ChangeAppCommodityKeyOnVMAndAppStage(applicationCommodityKeyChanger))
                .addStage(new EnvironmentTypeStage(environmentTypeInjector))
                .addStage(new IgnoreConstraintsStage(context.getGroupResolver(), groupServiceClient, changes))
                .addStage(new PolicyStage(policyManager, changes))
                .addStage(new ScopeResolutionStage(groupServiceClient, scope))
                .addStage(new CommoditiesEditStage(commoditiesEditor, changes, scope))
                .addStage(SettingsResolutionStage.plan(entitySettingsResolver, changes))
                .addStage(new SettingsApplicationStage(settingsApplicator))
                .addStage(new PostStitchingStage(stitchingManager))
                .addStage(new EntityValidationStage(entityValidator))
                .addStage(new HistoryAggregationStage(historyAggregator, changes, topologyInfo, scope))
                .addStage(new ExtractTopologyGraphStage())
                .addStage(new PlanScopingStage(planTopologyScopeEditor, scope, searchResolver, changes, groupServiceClient))
                .addStage(new HistoricalUtilizationStage(historicalEditor, changes))
                .addStage(new BroadcastStage(Collections.singletonList(topoBroadcastManager)))
                .build();
    }

    /**
     * Create a pipeline that acquires a previously-broadcast topology and applies a set of changes
     * to it. The main purpose of this is plan-over-plan.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @param changes The list of changes to apply.
     * @return The {@link TopologyPipeline}. This pipeline will accept a long (the ID of the old
     *         topology) and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    public TopologyPipeline<Long, TopologyBroadcastInfo> planOverOldTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<ScenarioChange> changes,
            @Nullable final PlanScope scope) {
        final TopologyPipelineContext context =
                new TopologyPipelineContext(new GroupResolver(searchResolver, groupServiceClient), topologyInfo);
        return TopologyPipeline.<Long, TopologyBroadcastInfo>newBuilder(context)
                .addStage(new TopologyAcquisitionStage(repositoryClient))
                .addStage(new TopologyEditStage(topologyEditor, searchResolver, changes, groupServiceClient))
                .addStage(new GraphCreationStage())
                .addStage(new ScopeResolutionStage(groupServiceClient, scope))
                .addStage(new CommoditiesEditStage(commoditiesEditor, changes, scope))
                // TODO (roman, Nov 2017): We need to do policy and setting application for
                // plan-over-plan as well. However, the topology we get from the repository
                // already has some policies and settings applied to it. In order to run those
                // stages here we need to be able to apply policies/settings on top of existing ones.
                //
                // One approach is to clear settings/policies from a topology, and then run the
                // stages. The other approach is to extend the stages to handle already-existing
                // policies/settings.
                .addStage(new BroadcastStage(Collections.singletonList(topoBroadcastManager)))
                .build();
    }

    /**
     * Class for caching the constructed topology from the live topology broadcast pipeline so it
     * can be used by the plan over live topology pipeline.
     */
    public class CachedTopology {
        /**
         * Holds a copy of the result from the most recent constructed topology stage run by the
         * live topology broadcast or null if none have run successfully yet.
         */
        private Map<Long, TopologyEntity.Builder> cachedMap;

        /**
         * Cache the result of the construct topology stage.
         *
         * @param newMap Topology map to be cached.
         */
        public synchronized void updateTopology(@Nonnull Map<Long, TopologyEntity.Builder> newMap) {
            cachedMap = newMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
        }

        /**
         * Return a deep copy of the cached topology.
         *
         * @return the most recently cached topology.
         */
        public synchronized Map<Long, TopologyEntity.Builder> getTopology() {
            return cachedMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
        }

        /**
         * Return true if no topology has been cached yet.
         *
         * @return boolean indicating if the cache is empty or not.
         */
        public synchronized boolean isEmpty() {
            return cachedMap == null || cachedMap.isEmpty();
        }
    }
}
