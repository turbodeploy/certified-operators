package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.EntityOids;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsUploader;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinitionBuilder;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingConfig;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.GroupResolverSearchFilterResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal.StitchingJournalContainer;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper;
import com.vmturbo.topology.processor.topology.CommoditiesEditor;
import com.vmturbo.topology.processor.topology.DemandOverriddenCommodityEditor;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor;
import com.vmturbo.topology.processor.topology.PostScopingTopologyEditor;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ApplyClusterCommodityStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CachingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ChangeAppCommodityKeyOnVMAndAppStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CloudMigrationPlanStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CommoditiesEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ControllableStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EntityValidationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EnvironmentTypeStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EphemeralEntityHistoryStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ExtractTopologyGraphStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GraphCreationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoricalUtilizationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoryAggregationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.IgnoreConstraintsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.InitializeTopologyEntitiesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.OverrideWorkLoadDemandStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PlanScopingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PolicyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PostScopingEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PostStitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ProbeActionCapabilitiesApplicatorStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.RequestAndLimitCommodityThresholdsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ReservationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScopeResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsApplicationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsUploadStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopSortStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyAcquisitionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadAtomicActionSpecsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UserScopingStage;

import common.HealthCheck.HealthState;

/**
 * A factory class for properly configured {@link TopologyPipeline} objects for plan topologies.
 *
 * <p>Users should not instantiate plan {@link TopologyPipeline}s themselves. Instead, they should
 * use the appropriately configured pipelines provided by this factory - e.g.
 * {@link PlanPipelineFactory#planOverLiveTopology(TopologyInfo, List, PlanScope, Map, StitchingJournalFactory)}.
 */
public class PlanPipelineFactory {
    private static final Logger logger = LogManager.getLogger();
    private static final TargetHealth DEFAULT_TARGET_HEALTH =
            TargetHealth.newBuilder().setHealthState(HealthState.NORMAL).build();

    private final TopoBroadcastManager topoBroadcastManager;

    private final PolicyManager policyManager;

    private final StitchingManager stitchingManager;

    private final EntitySettingsApplicator settingsApplicator;

    private final EntitySettingsResolver entitySettingsResolver;

    private final EnvironmentTypeInjector environmentTypeInjector;

    private final TopologyEditor topologyEditor;

    private final PostScopingTopologyEditor postScopingTopologyEditor;

    private final RepositoryClient repositoryClient;

    private final SearchResolver<TopologyEntity> searchResolver;

    private final GroupServiceBlockingStub groupServiceClient;

    private final ReservationManager reservationManager;

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

    private final ApplicationCommodityKeyChanger applicationCommodityKeyChanger;

    private final EntityValidator entityValidator;

    private final ConsistentScalingConfig consistentScalingConfig;

    private final CommoditiesEditor commoditiesEditor;

    private final PlanTopologyScopeEditor planTopologyScopeEditor;

    private final ProbeActionCapabilitiesApplicatorEditor applicatorEditor;

    private final HistoricalEditor historicalEditor;

    private final MatrixInterface matrix;

    private final CachedTopology constructTopologyStageCache;

    private final HistoryAggregator historyAggregator;

    private final DemandOverriddenCommodityEditor demandOverriddenCommodityEditor;

    private final RequestAndLimitCommodityThresholdsInjector requestAndLimitCommodityThresholdsInjector;

    private final EphemeralEntityEditor ephemeralEntityEditor;

    private final GroupResolverSearchFilterResolver searchFilterResolver;

    private final CloudMigrationPlanHelper cloudMigrationPlanHelper;

    private final ControllableManager controllableManager;

    private final ActionMergeSpecsUploader actionMergeSpecsUploader;

    private boolean firstPlanOverLive = true;

    private boolean firstPlanOverPlan = true;

    public PlanPipelineFactory(@Nonnull final TopoBroadcastManager topoBroadcastManager,
                               @Nonnull final PolicyManager policyManager,
                               @Nonnull final StitchingManager stitchingManager,
                               @Nonnull final EntitySettingsResolver entitySettingsResolver,
                               @Nonnull final EntitySettingsApplicator settingsApplicator,
                               @Nonnull final EnvironmentTypeInjector environmentTypeInjector,
                               @Nonnull final TopologyEditor topologyEditor,
                               @Nonnull final PostScopingTopologyEditor postScopingTopologyEditor,
                               @Nonnull final RepositoryClient repositoryClient,
                               @Nonnull final SearchResolver<TopologyEntity> searchResolver,
                               @Nonnull final GroupServiceBlockingStub groupServiceClient,
                               @Nonnull final ReservationManager reservationManager,
                               @Nonnull final EntityValidator entityValidator,
                               @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache,
                               @Nonnull final ApplicationCommodityKeyChanger applicationCommodityKeyChanger,
                               @Nonnull final CommoditiesEditor commoditiesEditor,
                               @Nonnull final PlanTopologyScopeEditor planTopologyScopeEditor,
                               @Nonnull final ProbeActionCapabilitiesApplicatorEditor applicatorEditor,
                               @Nonnull final HistoricalEditor historicalEditor,
                               @Nonnull final MatrixInterface matrix,
                               @Nonnull final CachedTopology constructTopologyStageCache,
                               @Nonnull HistoryAggregator historyAggregationStage,
                               @Nonnull DemandOverriddenCommodityEditor demandOverriddenCommodityEditor,
                               @Nonnull final ConsistentScalingConfig consistentScalingConfig,
                               @Nonnull final RequestAndLimitCommodityThresholdsInjector requestAndLimitCommodityThresholdsInjector,
                               @Nonnull final EphemeralEntityEditor ephemeralEntityEditor,
                               @Nonnull final GroupResolverSearchFilterResolver searchFilterResolver,
                               @Nonnull final CloudMigrationPlanHelper cloudMigrationPlanHelper,
                               @Nonnull final ControllableManager controllableManager,
                               @Nonnull final ActionMergeSpecsUploader actionMergeSpecsUploader) {
        this.topoBroadcastManager = topoBroadcastManager;
        this.policyManager = policyManager;
        this.stitchingManager = stitchingManager;
        this.settingsApplicator = Objects.requireNonNull(settingsApplicator);
        this.entitySettingsResolver = entitySettingsResolver;
        this.environmentTypeInjector = Objects.requireNonNull(environmentTypeInjector);
        this.topologyEditor = Objects.requireNonNull(topologyEditor);
        this.postScopingTopologyEditor = Objects.requireNonNull(postScopingTopologyEditor);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.reservationManager = Objects.requireNonNull(reservationManager);
        this.discoveredClusterConstraintCache = Objects.requireNonNull(discoveredClusterConstraintCache);
        this.applicationCommodityKeyChanger = Objects.requireNonNull(applicationCommodityKeyChanger);
        this.entityValidator = Objects.requireNonNull(entityValidator);
        this.consistentScalingConfig = Objects.requireNonNull(consistentScalingConfig);
        this.commoditiesEditor = Objects.requireNonNull(commoditiesEditor);
        this.planTopologyScopeEditor = planTopologyScopeEditor;
        this.applicatorEditor = applicatorEditor;
        this.historicalEditor = Objects.requireNonNull(historicalEditor);
        this.matrix = Objects.requireNonNull(matrix);
        this.constructTopologyStageCache = Objects.requireNonNull(constructTopologyStageCache);
        this.historyAggregator = Objects.requireNonNull(historyAggregationStage);
        this.demandOverriddenCommodityEditor = demandOverriddenCommodityEditor;
        this.requestAndLimitCommodityThresholdsInjector = Objects.requireNonNull(requestAndLimitCommodityThresholdsInjector);
        this.ephemeralEntityEditor = Objects.requireNonNull(ephemeralEntityEditor);
        this.searchFilterResolver = Objects.requireNonNull(searchFilterResolver);
        this.cloudMigrationPlanHelper = Objects.requireNonNull(cloudMigrationPlanHelper);
        this.controllableManager = Objects.requireNonNull(controllableManager);
        this.actionMergeSpecsUploader = Objects.requireNonNull(actionMergeSpecsUploader);
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
     * @param scope The scope of the Plan
     * @param userScopeEntityTypes  A list of accessible entities by entity type for
     *                              the User that requested the Plan.
     * @param journalFactory The journal factory to be used to create a journal to track changes made
     *                       during stitching.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    TopologyPipeline<PipelineInput, TopologyBroadcastInfo> planOverLiveTopology(
        @Nonnull final TopologyInfo topologyInfo,
        @Nonnull final List<ScenarioChange> changes,
        @Nullable final PlanScope scope,
        @Nullable final Map<Integer, EntityOids> userScopeEntityTypes,
        @Nonnull final StitchingJournalFactory journalFactory) {
        final TopologyPipelineContext context = new TopologyPipelineContext(topologyInfo);
        // if the constructed topology is already in the cache from the realtime topology, just
        // add the stage that will read it from the cache, otherwise add the stitching stage
        // and the construct topology stage so we can build the topology
        PipelineDefinitionBuilder<PipelineInput, TopologyBroadcastInfo, PipelineInput, TopologyPipelineContext> topoPipelineBuilder =
            PipelineDefinition.newBuilder(context);
        topoPipelineBuilder.initialContextMember(TopologyPipelineContextMembers.GROUP_RESOLVER,
            () -> new GroupResolver(searchResolver, groupServiceClient, searchFilterResolver));
        if (!constructTopologyStageCache.isEmpty()) {
            topoPipelineBuilder.initialContextMember(TopologyPipelineContextMembers.STITCHING_JOURNAL_CONTAINER,
                    StitchingJournalContainer::new);
        }

        PipelineDefinitionBuilder<PipelineInput, TopologyBroadcastInfo, Map<Long, TopologyEntity.Builder>, TopologyPipelineContext> builderContinuation;
        if (constructTopologyStageCache.isEmpty()) {
            builderContinuation = topoPipelineBuilder
                .addStage(new StitchingStage(stitchingManager, journalFactory,
                                new StitchingJournalContainer(),
                                new StalenessInformationProvider() {
                                    @Override
                                    public TargetHealth getLastKnownTargetHealth(long targetOid) {
                                        return DEFAULT_TARGET_HEALTH;
                                    }
                                }))
                .addStage(new ConstructTopologyFromStitchingContextStage())
                .addStage(new InitializeTopologyEntitiesStage());
        } else {
            builderContinuation = topoPipelineBuilder
                .addStage(new CachingConstructTopologyFromStitchingContextStage(constructTopologyStageCache))
                .addStage(new InitializeTopologyEntitiesStage());
        }
        final TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline = new TopologyPipeline<>(builderContinuation
            .addStage(new ReservationStage(reservationManager))
                // TODO: Move the ToplogyEditStage after the GraphCreationStage
                // That way the editstage can work on the graph instead of a
                // separate structure.
            .addStage(new TopologyEditStage(topologyEditor, searchResolver, scope, changes,
                                            groupServiceClient, searchFilterResolver))
            .addStage(new GraphCreationStage())
            .addStage(new ApplyClusterCommodityStage(discoveredClusterConstraintCache))
            .addStage(new ChangeAppCommodityKeyOnVMAndAppStage(applicationCommodityKeyChanger))
            .addStage(SegmentDefinition
                .addStage(new ScopeResolutionStage(groupServiceClient, scope))
                .addStage(new EnvironmentTypeStage(environmentTypeInjector))
                .addStage(new PlanScopingStage(planTopologyScopeEditor, scope, searchResolver, changes, groupServiceClient, searchFilterResolver))
                .addStage(new PostScopingEditStage(postScopingTopologyEditor, changes))
                .addStage(new UserScopingStage(userScopeEntityTypes))
                .finalStage(new CloudMigrationPlanStage(cloudMigrationPlanHelper, scope, changes))
                .asStage("ScopingSegment"))
            .addStage(SegmentDefinition
                .addStage(new PolicyStage(policyManager, changes))
                .addStage(new IgnoreConstraintsStage(groupServiceClient, changes))
                .addStage(new CommoditiesEditStage(commoditiesEditor, changes, scope))
                .addStage(SettingsResolutionStage.plan(entitySettingsResolver, changes, consistentScalingConfig))
                .addStage(new SettingsUploadStage(entitySettingsResolver))
                .finalStage(new SettingsApplicationStage(settingsApplicator))
                .asStage("SettingsAndPoliciesSegment"))
            .addStage(new PostStitchingStage(stitchingManager))
            .addStage(new ControllableStage(controllableManager))
            .addStage(new EntityValidationStage(entityValidator, true))
            .addStage(new HistoryAggregationStage(historyAggregator, changes, topologyInfo, scope))
            .addStage(new ExtractTopologyGraphStage())
            .addStage(new HistoricalUtilizationStage(historicalEditor, changes))
            .addStage(new OverrideWorkLoadDemandStage(demandOverriddenCommodityEditor, searchResolver, groupServiceClient, changes, searchFilterResolver))
            .addStage(new RequestAndLimitCommodityThresholdsStage(requestAndLimitCommodityThresholdsInjector))
            .addStage(new EphemeralEntityHistoryStage(ephemeralEntityEditor))
            .addStage(new ProbeActionCapabilitiesApplicatorStage(applicatorEditor))
            .addStage(new UploadAtomicActionSpecsStage(actionMergeSpecsUploader))
            .addStage(new TopSortStage())
            .finalStage(new BroadcastStage(Collections.singletonList(topoBroadcastManager), matrix)));

        if (firstPlanOverLive) {
            firstPlanOverLive = false;
            logger.info("\n" + pipeline.tabularDescription("PlanOverLive Topology Pipeline"));
        }
        return pipeline;
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
    TopologyPipeline<Long, TopologyBroadcastInfo> planOverOldTopology(
        @Nonnull final TopologyInfo topologyInfo,
        @Nonnull final List<ScenarioChange> changes,
        @Nullable final PlanScope scope) {
        final TopologyPipelineContext context = new TopologyPipelineContext(topologyInfo);
        final TopologyPipeline<Long, TopologyBroadcastInfo> pipeline = new TopologyPipeline<>(PipelineDefinition.<Long, TopologyBroadcastInfo, TopologyPipelineContext>newBuilder(context)
            .initialContextMember(TopologyPipelineContextMembers.GROUP_RESOLVER,
                () -> new GroupResolver(searchResolver, groupServiceClient, searchFilterResolver))
            .addStage(new TopologyAcquisitionStage(repositoryClient))
            .addStage(new TopologyEditStage(topologyEditor, searchResolver, scope, changes,
                                            groupServiceClient, searchFilterResolver))
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
            .addStage(new TopSortStage())
            .finalStage(new BroadcastStage(Collections.singletonList(topoBroadcastManager), matrix)));

        if (firstPlanOverPlan) {
            firstPlanOverPlan = false;
            logger.info("\n" + pipeline.tabularDescription("PlanOverPlan Topology Pipeline"));
        }
        return pipeline;
    }
}
