package com.vmturbo.topology.processor.topology.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.listeners.HistoryVolumesListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.actions.ActionConstraintsUploader;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsUploader;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingConfig;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.entity.EntityCustomTagsMerger;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.GroupResolverSearchFilterResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.listeners.TpAppSvcHistoryListener;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal.StitchingJournalContainer;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ApplyClusterCommodityStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CacheWritingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ChangeAppCommodityKeyOnVMAndAppStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ControllableStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.DummySettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EntityValidationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EnvironmentTypeStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.EphemeralEntityHistoryStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ExtractTopologyGraphStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GenerateConstraintMapStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GraphCreationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoricalUtilizationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoryAggregationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.InitializeTopologyEntitiesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.MergeEntityCustomTagsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PolicyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PostStitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ProbeActionCapabilitiesApplicatorStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.RequestAndLimitCommodityThresholdsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ReservationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScanDiscoveredSettingPoliciesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsApplicationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsUploadStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingGroupAnalyzerStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SupplyChainValidationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopSortStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UpdateAppServiceDaysEmptyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadBilledCloudCostDataStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.VolumesDaysUnAttachedCalcStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadActionConstraintsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadAtomicActionSpecsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadCloudCostDataStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadPlanDestinationsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadTemplatesStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadWorkflowsStage;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * A factory class for properly configured {@link TopologyPipeline} objects for live topologies.
 *
 * <p>Users should not instantiate live {@link TopologyPipeline}s themselves. Instead, they should
 * use the appropriately configured pipelines provided by this factory - e.g.
 * {@link LivePipelineFactory#liveTopology(TopologyInfo, List, StitchingJournalFactory)}.
 */
public class LivePipelineFactory {

    private static final Logger logger = LogManager.getLogger();

    private final TopoBroadcastManager topoBroadcastManager;

    private final PolicyManager policyManager;

    private final StitchingManager stitchingManager;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final DiscoveredWorkflowUploader discoveredWorkflowUploader;

    private final DiscoveredCloudCostUploader discoveredCloudCostUploader;

    private final BilledCloudCostUploader billedCloudCostUploader;

    private final DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader;

    private final EntitySettingsApplicator settingsApplicator;

    private final EntitySettingsResolver entitySettingsResolver;

    private final EnvironmentTypeInjector environmentTypeInjector;

    private final SearchResolver<TopologyEntity> searchResolver;

    private final GroupServiceBlockingStub groupServiceClient;

    private final ReservationManager reservationManager;

    private final DiscoveredSettingPolicyScanner discoveredSettingPolicyScanner;

    private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

    private final ApplicationCommodityKeyChanger applicationCommodityKeyChanger;

    private final EntityValidator entityValidator;

    private final SupplyChainValidator supplyChainValidator;

    private final ControllableManager controllableManager;

    private final HistoricalEditor historicalEditor;

    private final MatrixInterface matrix;

    private final CachedTopology constructTopologyStageCache;

    private final ProbeActionCapabilitiesApplicatorEditor applicatorEditor;

    private final HistoryAggregator historyAggregator;

    private final LicenseCheckClient licenseCheckClient;

    private final ConsistentScalingConfig consistentScalingConfig;

    private final ActionConstraintsUploader actionConstraintsUploader;

    private final ActionMergeSpecsUploader actionMergeSpecsUploader;

    private final RequestAndLimitCommodityThresholdsInjector requestAndLimitCommodityThresholdsInjector;

    private final EphemeralEntityEditor ephemeralEntityEditor;

    private final ReservationServiceStub reservationService;

    private final GroupResolverSearchFilterResolver searchFilterResolver;

    private final GroupScopeResolver groupScopeResolver;

    private final EntityCustomTagsMerger entityCustomTagsMerger;

    private final StalenessInformationProvider stalenessProvider;

    private final int supplyChainValidationFrequency;

    private long broadcastCount = 0;

    private final HistoryVolumesListener histListener;

    private final TpAppSvcHistoryListener appSvcHistoryListener;

    public LivePipelineFactory(@Nonnull final TopoBroadcastManager topoBroadcastManager,
            @Nonnull final PolicyManager policyManager,
            @Nonnull final StitchingManager stitchingManager,
            @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
            @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
            @Nonnull final DiscoveredWorkflowUploader discoveredWorkflowUploader,
            @Nonnull final DiscoveredCloudCostUploader cloudCostUploader,
            @Nonnull final BilledCloudCostUploader billedCloudCostUploader,
            @Nonnull final DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader,
            @Nonnull final EntitySettingsResolver entitySettingsResolver,
            @Nonnull final EntitySettingsApplicator settingsApplicator,
            @Nonnull final EnvironmentTypeInjector environmentTypeInjector,
            @Nonnull final SearchResolver<TopologyEntity> searchResolver,
            @Nonnull final GroupServiceBlockingStub groupServiceClient,
            @Nonnull final ReservationManager reservationManager,
            @Nonnull final DiscoveredSettingPolicyScanner discoveredSettingPolicyScanner,
            @Nonnull final EntityValidator entityValidator,
            @Nonnull final SupplyChainValidator supplyChainValidator,
            @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache,
            @Nonnull final ApplicationCommodityKeyChanger applicationCommodityKeyChanger,
            @Nonnull final ControllableManager controllableManager,
            @Nonnull final HistoricalEditor historicalEditor,
            @Nonnull final MatrixInterface matrix,
            @Nonnull final CachedTopology constructTopologyStageCache,
            @Nonnull final ProbeActionCapabilitiesApplicatorEditor applicatorEditor,
            @Nonnull HistoryAggregator historyAggregationStage,
            @Nonnull final LicenseCheckClient licenseCheckClient,
            @Nonnull final ConsistentScalingConfig consistentScalingConfig,
            @Nonnull final ActionConstraintsUploader actionConstraintsUploader,
            @Nonnull final ActionMergeSpecsUploader actionMergeSpecsUploader,
            @Nonnull final RequestAndLimitCommodityThresholdsInjector requestAndLimitCommodityThresholdsInjector,
            @Nonnull final EphemeralEntityEditor ephemeralEntityEditor,
            @Nonnull final ReservationServiceStub reservationService,
            @Nonnull final GroupResolverSearchFilterResolver searchFilterResolver,
            @Nonnull final GroupScopeResolver groupScopeResolver,
            @Nonnull final EntityCustomTagsMerger entityCustomTagsMerger,
            @Nonnull StalenessInformationProvider stalenessProvider,
            final int supplyChainValidationFrequency,
            final HistoryVolumesListener histListener,
            final TpAppSvcHistoryListener appSvcHistoryListener) {
        this.topoBroadcastManager = topoBroadcastManager;
        this.policyManager = policyManager;
        this.stitchingManager = stitchingManager;
        this.discoveredTemplateDeploymentProfileNotifier = discoveredTemplateDeploymentProfileNotifier;
        this.discoveredGroupUploader = discoveredGroupUploader;
        this.discoveredWorkflowUploader = discoveredWorkflowUploader;
        this.discoveredCloudCostUploader = cloudCostUploader;
        this.billedCloudCostUploader = Objects.requireNonNull(billedCloudCostUploader);
        this.discoveredPlanDestinationUploader = discoveredPlanDestinationUploader;
        this.settingsApplicator = Objects.requireNonNull(settingsApplicator);
        this.entitySettingsResolver = entitySettingsResolver;
        this.environmentTypeInjector = Objects.requireNonNull(environmentTypeInjector);
        this.searchResolver = Objects.requireNonNull(searchResolver);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.reservationManager = Objects.requireNonNull(reservationManager);
        this.discoveredSettingPolicyScanner = Objects.requireNonNull(discoveredSettingPolicyScanner);
        this.discoveredClusterConstraintCache = Objects.requireNonNull(discoveredClusterConstraintCache);
        this.applicationCommodityKeyChanger = Objects.requireNonNull(applicationCommodityKeyChanger);
        this.entityValidator = Objects.requireNonNull(entityValidator);
        this.supplyChainValidator = Objects.requireNonNull(supplyChainValidator);
        this.controllableManager = Objects.requireNonNull(controllableManager);
        this.historicalEditor = Objects.requireNonNull(historicalEditor);
        this.matrix = Objects.requireNonNull(matrix);
        this.constructTopologyStageCache = Objects.requireNonNull(constructTopologyStageCache);
        this.applicatorEditor = Objects.requireNonNull(applicatorEditor);
        this.historyAggregator = Objects.requireNonNull(historyAggregationStage);
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.consistentScalingConfig = Objects.requireNonNull(consistentScalingConfig);
        this.actionConstraintsUploader = actionConstraintsUploader;
        this.requestAndLimitCommodityThresholdsInjector = Objects.requireNonNull(requestAndLimitCommodityThresholdsInjector);
        this.actionMergeSpecsUploader = actionMergeSpecsUploader;
        this.ephemeralEntityEditor = Objects.requireNonNull(ephemeralEntityEditor);
        this.reservationService = Objects.requireNonNull(reservationService);
        this.searchFilterResolver = Objects.requireNonNull(searchFilterResolver);
        this.groupScopeResolver = Objects.requireNonNull(groupScopeResolver);
        this.supplyChainValidationFrequency = supplyChainValidationFrequency;
        this.entityCustomTagsMerger = entityCustomTagsMerger;
        this.stalenessProvider = stalenessProvider;
        this.histListener = histListener;
        this.appSvcHistoryListener = appSvcHistoryListener;
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
    TopologyPipeline<EntityStore, TopologyBroadcastInfo> liveTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<TopoBroadcastManager> additionalBroadcastManagers,
            @Nonnull final StitchingJournalFactory journalFactory) {
        final TopologyPipeline<EntityStore, TopologyBroadcastInfo> liveTopology =
            buildLiveTopology(topologyInfo, additionalBroadcastManagers, journalFactory);
        if (broadcastCount == 1) {
            logger.info("\n" + liveTopology.tabularDescription("Live Topology Pipeline"));
        }
        return liveTopology;
    }

    /**
     * Build the live topology.
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
    private TopologyPipeline<EntityStore, TopologyBroadcastInfo> buildLiveTopology(
        @Nonnull TopologyInfo topologyInfo,
        @Nonnull List<TopoBroadcastManager> additionalBroadcastManagers,
        @Nonnull StitchingJournalFactory journalFactory) {
        final TopologyPipelineContext context = new TopologyPipelineContext(topologyInfo);
        final List<TopoBroadcastManager> managers = new ArrayList<>(additionalBroadcastManagers.size() + 1);
        managers.add(topoBroadcastManager);
        managers.addAll(additionalBroadcastManagers);
        broadcastCount += 1;
        if (licenseCheckClient.isDevFreemium()) {
            return liveDevFreemiumTopology(context, journalFactory, managers);
        }
        return liveXLTopology(topologyInfo, context, journalFactory, managers);
    }

    /**
     * Create a pipeline that constructs and broadcasts the full live topology.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @param context A context object shared by all stages in the pipeline.
     * @param journalFactory The journal factory to be used to create a journal to track changes made
     *                       during stitching.
     * @param managers A list of topology broadcast managers.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    private TopologyPipeline<EntityStore, TopologyBroadcastInfo> liveXLTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final TopologyPipelineContext context,
            @Nonnull final StitchingJournalFactory journalFactory,
            @Nonnull final List<TopoBroadcastManager> managers) {
        final MatrixInterface mi = matrix.copy();
        return new TopologyPipeline<>(PipelineDefinition.<EntityStore, TopologyBroadcastInfo, TopologyPipelineContext>newBuilder(context)
                .initialContextMember(TopologyPipelineContextMembers.GROUP_RESOLVER,
                    () -> new GroupResolver(searchResolver, groupServiceClient, searchFilterResolver))
                .addStage(new DCMappingStage(discoveredGroupUploader))
                .addStage(new StitchingStage(stitchingManager, journalFactory,
                                new StitchingJournalContainer(), stalenessProvider))
                .addStage(new Stages.FlowGenerationStage(mi))
                .addStage(new UploadCloudCostDataStage(discoveredCloudCostUploader))
                .addStage(new UploadBilledCloudCostDataStage(billedCloudCostUploader))
                .addStage(new UploadPlanDestinationsStage(discoveredPlanDestinationUploader))
                .addStage(new ScanDiscoveredSettingPoliciesStage(discoveredSettingPolicyScanner,
                        discoveredGroupUploader))
                .addStage(new UploadActionConstraintsStage(actionConstraintsUploader, groupServiceClient))
                .addStage(new StitchingGroupAnalyzerStage(discoveredGroupUploader))
                .addStage(new CacheWritingConstructTopologyFromStitchingContextStage(constructTopologyStageCache))
                .addStage(new InitializeTopologyEntitiesStage())
                .addStage(new MergeEntityCustomTagsStage(entityCustomTagsMerger))
                .addStage(new UploadGroupsStage(discoveredGroupUploader))
                .addStage(new UploadWorkflowsStage(discoveredWorkflowUploader))
                .addStage(new UploadTemplatesStage(discoveredTemplateDeploymentProfileNotifier))
                .addStage(new ReservationStage(reservationManager))
                .addStage(new GraphCreationStage(groupScopeResolver))
                .addStage(new ApplyClusterCommodityStage(discoveredClusterConstraintCache))
                .addStage(new ChangeAppCommodityKeyOnVMAndAppStage(applicationCommodityKeyChanger))
                .addStage(new EnvironmentTypeStage(environmentTypeInjector))
                .addStage(new VolumesDaysUnAttachedCalcStage(histListener))
                .addStage(new UpdateAppServiceDaysEmptyStage(appSvcHistoryListener))
                .addStage(SegmentDefinition
                    .addStage(new PolicyStage(policyManager))
                    .addStage(new GenerateConstraintMapStage(policyManager, groupServiceClient, reservationService))
                    .addStage(SettingsResolutionStage.live(entitySettingsResolver, consistentScalingConfig))
                    .addStage(new SettingsUploadStage(entitySettingsResolver))
                    .finalStage(new SettingsApplicationStage(settingsApplicator))
                    .asStage("SettingsAndPoliciesSegment"))
                .addStage(new Stages.MatrixUpdateStage(mi))
                .addStage(new PostStitchingStage(stitchingManager))
                .addStage(new ControllableStage(controllableManager))
                .addStage(new EntityValidationStage(entityValidator, false))
                .addStage(new SupplyChainValidationStage(supplyChainValidator,
                    supplyChainValidationFrequency, broadcastCount))
                .addStage(new HistoryAggregationStage(historyAggregator, null, topologyInfo, null))
                .addStage(new ExtractTopologyGraphStage())
                .addStage(new HistoricalUtilizationStage(historicalEditor))
                .addStage(new RequestAndLimitCommodityThresholdsStage(requestAndLimitCommodityThresholdsInjector))
                .addStage(new EphemeralEntityHistoryStage(ephemeralEntityEditor))
                .addStage(new ProbeActionCapabilitiesApplicatorStage(applicatorEditor))
                .addStage(new UploadAtomicActionSpecsStage(actionMergeSpecsUploader))
                .addStage(new TopSortStage())
                .finalStage(new BroadcastStage(managers, mi)));
    }

    /**
     * Create a pipeline that constructs and broadcasts the minimum live topology used
     * in the developer freemium edition. A {@link DummySettingsResolutionStage} is inserted
     * to the pipeline stages which takes the {@link com.vmturbo.topology.graph.TopologyGraph}
     * as input and produce {@link GraphWithSettings}
     * with empty settings as output.
     *
     * @param context A context object shared by all stages in the pipeline.
     * @param journalFactory The journal factory to be used to create a journal to track changes made
     *                       during stitching.
     * @param managers A list of topology broadcast managers.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    private TopologyPipeline<EntityStore, TopologyBroadcastInfo> liveDevFreemiumTopology(
            @Nonnull final TopologyPipelineContext context,
            @Nonnull final StitchingJournalFactory journalFactory,
            @Nonnull final List<TopoBroadcastManager> managers) {
        return new TopologyPipeline<>(PipelineDefinition.<EntityStore, TopologyBroadcastInfo, TopologyPipelineContext>newBuilder(context)
            .initialContextMember(TopologyPipelineContextMembers.GROUP_RESOLVER,
                () -> new GroupResolver(searchResolver, groupServiceClient, searchFilterResolver))
            .addStage(new DCMappingStage(discoveredGroupUploader))
            .addStage(new StitchingStage(stitchingManager, journalFactory,
                            new StitchingJournalContainer(), stalenessProvider))
            .addStage(new Stages.FlowGenerationStage(matrix))
            .addStage(new ScanDiscoveredSettingPoliciesStage(discoveredSettingPolicyScanner,
                    discoveredGroupUploader))
            .addStage(new StitchingGroupAnalyzerStage(discoveredGroupUploader))
            .addStage(new ConstructTopologyFromStitchingContextStage())
            .addStage(new InitializeTopologyEntitiesStage())
            .addStage(new UploadGroupsStage(discoveredGroupUploader))
            .addStage(new GraphCreationStage(groupScopeResolver))
            .addStage(new ApplyClusterCommodityStage(discoveredClusterConstraintCache))
            .addStage(new ChangeAppCommodityKeyOnVMAndAppStage(applicationCommodityKeyChanger))
            .addStage(new EnvironmentTypeStage(environmentTypeInjector))
            .addStage(new GenerateConstraintMapStage(policyManager, groupServiceClient, reservationService))
            .addStage(new DummySettingsResolutionStage())
            .addStage(new Stages.MatrixUpdateStage(matrix))
            .addStage(new PostStitchingStage(stitchingManager))
            .addStage(new SupplyChainValidationStage(supplyChainValidator, supplyChainValidationFrequency, broadcastCount ))
            .addStage(new ExtractTopologyGraphStage())
            .addStage(new TopSortStage())
            .finalStage(new BroadcastStage(managers, matrix)));
    }
}
