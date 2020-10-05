package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.ncm.MatrixDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingMetrics;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.actions.ActionConstraintsUploader;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsUploader;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.consistentscaling.ConsistentScalingManager;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.entity.EntitiesValidationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.GroupResolverSearchFilterResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.group.settings.SettingOverrides;
import com.vmturbo.topology.processor.ncm.FlowCommoditiesGenerator;
import com.vmturbo.topology.processor.reservation.GenerateConstraintMap;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationFailure;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader.UploadException;
import com.vmturbo.topology.processor.topology.ApplicationCommodityKeyChanger;
import com.vmturbo.topology.processor.topology.CloudMigrationPlanHelper;
import com.vmturbo.topology.processor.topology.CommoditiesEditor;
import com.vmturbo.topology.processor.topology.ConstraintsEditor;
import com.vmturbo.topology.processor.topology.ConstraintsEditor.ConstraintsEditorException;
import com.vmturbo.topology.processor.topology.DemandOverriddenCommodityEditor;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector;
import com.vmturbo.topology.processor.topology.EnvironmentTypeInjector.InjectionSummary;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor;
import com.vmturbo.topology.processor.topology.EphemeralEntityEditor.EditSummary;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregator;
import com.vmturbo.topology.processor.topology.PlanTopologyScopeEditor;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor;
import com.vmturbo.topology.processor.topology.ProbeActionCapabilitiesApplicatorEditor.EditorSummary;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology.CachedTopologyResult;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * A wrapper class for the various {@link Stage} and {@link PassthroughStage} implementations.
 * Since the stages are pretty small, it makes some sense to keep them in one place for now.
 */
public class Stages {
    private static final Logger logger = LogManager.getLogger();

    /**
     * This stage uploads cloud cost data to the cost component. We are doing this before the other
     * uploads, because all of the data we need to upload is available from the stitching context,
     * but not in the topology created from the stitching context.
     * <p>
     * We could cache the data somewhere and upload it during a later stage, such as when the groups
     * and workflows are uploaded, so that the "upload" stages can be grouped, but this will require
     * extra coding to hand the cloud cost data between stages, or to sync the cache in the cloud
     * cost uploader between stages. It is simpler to upload the data now rather than passing this
     * extra data for later processing though, so we'll do the upload now.
     */
    public static class UploadCloudCostDataStage extends PassthroughStage<StitchingContext> {

        private final DiscoveredCloudCostUploader cloudCostUploader;

        public UploadCloudCostDataStage(@Nonnull final DiscoveredCloudCostUploader cloudCostUploader) {
            this.cloudCostUploader = cloudCostUploader;
        }

        @NotNull
        @Override
        public Status passthrough(final StitchingContext input) throws PipelineStageException {
            // upload the cloud-related cost data to the cost component
            cloudCostUploader.uploadCostData(getContext().getTopologyInfo(), input);
            // TODO (roman, Oct 23 2018): Provide some additional information regarding
            // the cost upload - e.g. how many things got uploaded, did anything get skipped,
            // and so on.
            return Status.success();
        }
    }

    /**
     * This stage uploads discovered groups (and clusters) to the group component.
     * We only do this for the live topology.
     */
    public static class UploadGroupsStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {

        private final DiscoveredGroupUploader discoveredGroupUploader;

        public UploadGroupsStage(@Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
            this.discoveredGroupUploader = discoveredGroupUploader;
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(final Map<Long, TopologyEntity.Builder> input) {
            discoveredGroupUploader.uploadDiscoveredGroups(input,
                    getContext().getConsistentScalingManager());
            // TODO (roman, Oct 23 2018): Provide some additional information regarding
            // the group upload - e.g. how many groups got uploaded, did anything get skipped,
            // and so on.
            return Status.success();
        }
    }

    /**
     * This stage uploads discovered Workflows to the action-orchestrator component.
     */
    public static class UploadWorkflowsStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {

        private static final Logger logger = LogManager.getLogger();

        private final DiscoveredWorkflowUploader discoveredWorkflowUploader;

        public UploadWorkflowsStage(@Nonnull final DiscoveredWorkflowUploader discoveredWorkflowUploader) {
            this.discoveredWorkflowUploader = discoveredWorkflowUploader;
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(final Map<Long, TopologyEntity.Builder> input) {
            discoveredWorkflowUploader.uploadDiscoveredWorkflows();
            // TODO (roman, Oct 23 2018): Provide some additional information regarding
            // the workflow upload - e.g. how many workflows got uploaded, did anything get skipped,
            // and so on.
            return Status.success();
        }
    }

    /**
     * This stage uploads discovered templates and deployment profiles to the plan orchestrator.
     * We only do this for the live topology.
     */
    public static class UploadTemplatesStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {

        private final DiscoveredTemplateDeploymentProfileNotifier notifier;

        public UploadTemplatesStage(@Nonnull final DiscoveredTemplateDeploymentProfileNotifier notifier) {
            this.notifier = notifier;
        }

        @NotNull
        @Override
        @Nonnull
        public Status passthrough(final Map<Long, TopologyEntity.Builder> topology)
                throws InterruptedException {
            try {
                notifier.sendTemplateDeploymentProfileData();
                notifier.patchTopology(topology);
                return Status.success();
            } catch (UploadException e) {
                return Status.failed("Failed to upload templates: " + e.getMessage());
            }
        }
    }

    /**
     * This stage stitches together the various sub-topologies discovered by individual targets
     * into a unified "complete" topology.
     * <p>
     * Stitching can happen in both the live topology broadcast and the plan topology broadcast if
     * the plan is on top of the "live" topology.
     */
    public static class StitchingStage extends Stage<EntityStore, StitchingContext> {

        /**
         * A metric that tracks duration of preparation for stitching.
         */
        private static final DataMetricSummary STITCHING_PREPARATION_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("tp_stitching_preparation_duration_seconds")
            .withHelp("Duration of construction for data structures in preparation for stitching.")
            .build()
            .register();

        private final StitchingManager stitchingManager;
        private final StitchingJournalFactory journalFactory;

        public StitchingStage(@Nonnull final StitchingManager stitchingManager,
                              @Nonnull final StitchingJournalFactory journalFactory) {
            this.stitchingManager = stitchingManager;
            this.journalFactory = Objects.requireNonNull(journalFactory);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<StitchingContext> execute(@NotNull @Nonnull final EntityStore entityStore) {
            final DataMetricTimer preparationTimer = STITCHING_PREPARATION_DURATION_SUMMARY.startTimer();
            final StitchingContext stitchingContext = entityStore.constructStitchingContext();
            preparationTimer.observe();

            final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
            journal.recordTopologySizes(stitchingContext.entityTypeCounts());

            if (journal.shouldDumpTopologyBeforePreStitching()) {
                journal.dumpTopology(stitchingContext.getStitchingGraph().entities()
                    .map(Function.identity()));
            }

            getContext().getStitchingJournalContainer().setMainStitchingJournal(journal);
            final StitchingContext context = stitchingManager.stitch(stitchingContext, journal);

            final StitchingMetrics metrics = journal.getMetrics();
            final String status = new StringBuilder()
                    .append(metrics.getTotalChangesetsGenerated())
                    .append(" changesets generated\n")
                    .append(metrics.getTotalChangesetsIncluded())
                    .append(" changesets included\n")
                    .append(metrics.getTotalEmptyChangests())
                    .append(" empty changesets\n")
                    .toString();
            entityStore.sendMetricsEntityAndTargetData(stitchingContext);
            return StageResult.withResult(context)
                .andStatus(Status.success(status));
        }
    }

    /**
     * A stage that fixes up groups that references entities whose identifiers were modified
     * during stitching. See {@link StitchingGroupFixer} for more details.
     * <p>
     * TODO: (DavidBlinn 2/16/2018) Ideally, instead of being a separate stage, this should
     * be part of stitching itself. Unfortunately, because it has to be run with the Live
     * pipeline but cannot be run in the PlanOverLive topology we have to split it out.
     */
    public static class StitchingGroupFixupStage extends PassthroughStage<StitchingContext> {

        private final StitchingGroupFixer stitchingGroupFixer;
        private final DiscoveredGroupUploader discoveredGroupUploader;

        public StitchingGroupFixupStage(@Nonnull final StitchingGroupFixer stitchingGroupFixer,
                                        @Nonnull final DiscoveredGroupUploader groupUploader) {
            this.stitchingGroupFixer = Objects.requireNonNull(stitchingGroupFixer);
            this.discoveredGroupUploader = Objects.requireNonNull(groupUploader);
        }

        @NotNull
        @Override
        public Status passthrough(StitchingContext input) throws PipelineStageException {
            final int fixedUpGroups = stitchingGroupFixer.fixupGroups(input.getStitchingGraph(),
                discoveredGroupUploader.buildMemberCache());
            return Status.success("Fixed up " + fixedUpGroups + " groups.");
        }
    }

    /**
     * This stage scans service entities for discovered setting policies to be uploaded.
     * See {@link com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage}.
     * <p>
     * This stage only happens in the live topology broadcast.
     */
    public static class ScanDiscoveredSettingPoliciesStage extends PassthroughStage<StitchingContext> {

        private static final Logger logger = LogManager.getLogger();

        private final DiscoveredGroupUploader discoveredGroupUploader;
        private final DiscoveredSettingPolicyScanner settingPolicyScanner;

        public ScanDiscoveredSettingPoliciesStage(@Nonnull final DiscoveredSettingPolicyScanner scanner,
                                                  @Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
            this.settingPolicyScanner = Objects.requireNonNull(scanner);
            this.discoveredGroupUploader = Objects.requireNonNull(discoveredGroupUploader);
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(@Nonnull final StitchingContext stitchingContext) {
            Status status;
            try {
                settingPolicyScanner.scanForDiscoveredSettingPolicies(stitchingContext,
                    discoveredGroupUploader);
                // TODO (roman, Oct 23 2018): Record the number of discovered groups and setting
                // policies in the status.
                status = Status.success();
            } catch (RuntimeException e) {
                // This stage is not required, but it's not quite a passthrough either. If an
                // exception occurs, log a warning and continue.
                status = Status.withWarnings(e.getMessage());
            }
            return status;
        }
    }

    /**
     * This stage modifies the ApplicationCommodity key on Apps and VMs, in order to make them
     * unique across the whole environment.
     * See {@link ApplicationCommodityKeyChanger} for further details.
     */
    public static class ChangeAppCommodityKeyOnVMAndAppStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final ApplicationCommodityKeyChanger applicationCommodityKeyChanger;

        public ChangeAppCommodityKeyOnVMAndAppStage(
            @Nonnull final ApplicationCommodityKeyChanger applicationCommodityKeyChanger) {
            this.applicationCommodityKeyChanger = applicationCommodityKeyChanger;
        }

        @NotNull
        @Override
        public Status passthrough(final TopologyGraph<TopologyEntity> topologyGraph) {
            final Map<ApplicationCommodityKeyChanger.KeyChangeOutcome, Integer> keysChangeCounts
                = applicationCommodityKeyChanger.execute(topologyGraph);
            final String summary = keysChangeCounts.entrySet().stream()
                .map((e) -> String.format("%s: %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining("; "));
            return Status.success("Commodity key change counts: " + summary);
        }
    }

    /**
     * This stage is for adding cluster commodities to relate TopologyEntityDTO.
     */
    public static class ApplyClusterCommodityStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {
        private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

        public ApplyClusterCommodityStage(
            @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache) {
            this.discoveredClusterConstraintCache = discoveredClusterConstraintCache;
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
            discoveredClusterConstraintCache.applyClusterCommodity(topologyGraph);
            // TODO (roman, Oct 23 2018): Provide some information about modified commodities.
            return Status.success();
        }
    }

    /**
     * This stage is for generating a map from constraints to commodities.
     */
    public static class GenerateConstraintMapStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {
        private final GenerateConstraintMap generateConstraintMap;

        /**
         * constructor for GenerateConstraintMapStage.
         *
         * @param policyManager      policy manager to get policy details
         * @param groupServiceClient group service to get cluster and datacenter information
         * @param reservationService reservation service to update the PO with the constraint map.
         */
        public GenerateConstraintMapStage(
                @Nonnull final PolicyManager policyManager,
                @Nonnull final GroupServiceBlockingStub groupServiceClient,
                @Nonnull final ReservationServiceBlockingStub reservationService
        ) {
            this.generateConstraintMap = new GenerateConstraintMap(policyManager,
                    groupServiceClient, reservationService);
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
            return generateConstraintMap.createMap(topologyGraph, getContext().getGroupResolver());
        }
    }


    /**
     * This stage uploads action constraints to the action orchestrator component.
     * We only do this for the live topology. Plan uses the same action constraints as real time.
     */
    public static class UploadActionConstraintsStage extends PassthroughStage<StitchingContext> {

        private final ActionConstraintsUploader actionConstraintsUploader;

        UploadActionConstraintsStage(
                @Nonnull final ActionConstraintsUploader actionConstraintsUploader) {
            this.actionConstraintsUploader = actionConstraintsUploader;
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(@Nonnull final StitchingContext stitchingContext) {
            actionConstraintsUploader.uploadActionConstraintInfo(stitchingContext);
            return Status.success();
        }
    }

    /**
     * This stage uploads action merge specs to the action orchestrator component.
     * We only do this for the live topology.
     */
    public static class UploadAtomicActionSpecsStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final ActionMergeSpecsUploader actionMergeSpecsUploader;

        UploadAtomicActionSpecsStage(@Nonnull final ActionMergeSpecsUploader actionMergeSpecsUploader) {
            this.actionMergeSpecsUploader = actionMergeSpecsUploader;
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
            actionMergeSpecsUploader.uploadAtomicActionSpecsInfo(topologyGraph);
            return Status.success();
        }
    }

    /**
     * This stage construct a topology map (ie OID -> TopologyEntity.Builder) from a {@Link StitchingContext}.
     */
    public static class ConstructTopologyFromStitchingContextStage
        extends Stage<StitchingContext, Map<Long, TopologyEntity.Builder>> {

        public ConstructTopologyFromStitchingContextStage() {

        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<Map<Long, TopologyEntity.Builder>> execute(@NotNull @Nonnull final StitchingContext stitchingContext) {
            final Map<Long, TopologyEntity.Builder> topology = stitchingContext.constructTopology();
            return StageResult.withResult(topology)
                .andStatus(Status.success("Constructed topology of size " + topology.size() +
                    " from context of size " + stitchingContext.size()));
        }
    }

    /**
     * This stage carries out the same function as ConstructTopologyFromStitchingContextStage, with
     * the additional feature that it writes out the result of its execution to a shared cache
     * fed in by the TopologyPipelineFactory.  This cache can be used by plan over live topology
     * pipelines to avoid having to run this stage.
     */
    public static class CacheWritingConstructTopologyFromStitchingContextStage extends
        ConstructTopologyFromStitchingContextStage {

        /**
         * Shared cache to write result into so it can be read from cache reading stage.
         */
        private final CachedTopology resultCache;

        CacheWritingConstructTopologyFromStitchingContextStage(
            @Nonnull CachedTopology resultCache) {
            this.resultCache = Objects.requireNonNull(resultCache);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<Map<Long, Builder>> execute(@NotNull @Nonnull final StitchingContext stitchingContext) {
            StageResult<Map<Long, Builder>> stageResult = super.execute(stitchingContext);
            if (stageResult.getStatus().getType() == Status.success().getType()) {
                resultCache.updateTopology(stageResult.getResult());
            }
            return stageResult;
        }
    }

    /**
     * This stage uses a cached result from {@link CacheWritingConstructTopologyFromStitchingContextStage}
     * to avoid having to redo the expensive operations of {@link StitchingStage} and
     * {@link ConstructTopologyFromStitchingContextStage} when running a plan over live topology.
     * This stage is only added to pipelines after checking that the cache is not empty.
     */
    public static class CachingConstructTopologyFromStitchingContextStage
        extends Stage<EntityStore, Map<Long, TopologyEntity.Builder>> {

        /**
         * Cache that will be filled by CacheWritingConstructTopologyFromStitchingContextStage
         */
        private final CachedTopology resultCache;

        public CachingConstructTopologyFromStitchingContextStage(
            @Nonnull final CachedTopology resultCache) {
            this.resultCache = Objects.requireNonNull(resultCache);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<Map<Long, Builder>> execute(@NotNull @Nonnull final EntityStore entityStore) {
            final CachedTopologyResult result = resultCache.getTopology();
            return StageResult.withResult(result.getEntities())
                .andStatus(Status.success(result.toString()));
        }
    }

    /**
     * This stage acquires an old topology from the repository.
     * <p>
     * We only do this in plans, because the live topology pipeline doesn't depend
     * on old topologies.
     */
    public static class TopologyAcquisitionStage extends Stage<Long, Map<Long, TopologyEntity.Builder>> {

        private final RepositoryClient repository;

        public TopologyAcquisitionStage(@Nonnull final RepositoryClient repositoryClient) {
            this.repository = Objects.requireNonNull(repositoryClient);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<Map<Long, TopologyEntity.Builder>> execute(@NotNull @Nonnull final Long topologyId) {
            // we need to gather the entire topology in order to perform editing below
            Iterable<RepositoryDTO.RetrieveTopologyResponse> dtos =
                () -> repository.retrieveTopology(topologyId);
            final Map<Long, TopologyEntity.Builder> topology =
                StreamSupport.stream(dtos.spliterator(), false)
                    .map(RepositoryDTO.RetrieveTopologyResponse::getEntitiesList)
                    .flatMap(List::stream)
                    .map(PartialEntity::getFullEntity)
                    .map(TopologyEntityDTO::toBuilder)
                    .collect(Collectors.toMap(TopologyEntityDTOOrBuilder::getOid,
                        // TODO: Persist and pass through discovery information for this entity.
                        TopologyEntity::newBuilder));
            return StageResult.withResult(topology)
                .andStatus(Status.success("Retrieved topology of size " + topology.size()));
        }
    }

    /**
     * This stage applies a set of {@link ScenarioChange} objects to a topology.
     * <p>
     * We only do this in plans because only plans have scenario changes on top of a topology.
     */
    public static class TopologyEditStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {

        private final TopologyEditor topologyEditor;
        private final List<ScenarioChange> changes;
        private final SearchResolver<TopologyEntity> searchResolver;
        private final GroupServiceBlockingStub groupServiceClient;
        private final GroupResolverSearchFilterResolver searchFilterResolver;

        public TopologyEditStage(@Nonnull final TopologyEditor topologyEditor,
                                 @Nonnull final SearchResolver<TopologyEntity> searchResolver,
                                 @Nonnull final List<ScenarioChange> scenarioChanges,
                                 @Nullable final GroupServiceBlockingStub groupServiceClient,
                                 @Nonnull final GroupResolverSearchFilterResolver searchFilterResolver) {
            this.topologyEditor = Objects.requireNonNull(topologyEditor);
            this.changes = Objects.requireNonNull(scenarioChanges);
            this.searchResolver = Objects.requireNonNull(searchResolver);
            this.groupServiceClient = groupServiceClient;
            this.searchFilterResolver = Objects.requireNonNull(searchFilterResolver);
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull final Map<Long, TopologyEntity.Builder> input)
                throws PipelineStageException {
            // Topology editing should use a group resolver distinct from the rest of the pipeline.
            // This is so that pre-edit group membership lookups don't get cached in the "main"
            // group resolver, preventing post-edit group membership lookups from seeing members
            // added or removed during editing.
            final GroupResolver groupResolver = new GroupResolver(searchResolver, groupServiceClient,
                    searchFilterResolver);
            try {
                topologyEditor.editTopology(input, changes, getContext(), groupResolver);
            } catch (GroupResolutionException e) {
                throw new PipelineStageException(e);
            } catch (TopologyEditorException e) {
                throw new PipelineStageException(e);
            }
            // TODO (roman, Oct 23 2018): Add some information about the number/type of
            // modifications made.
            return Status.success();
        }

        @Override
        public boolean required() {
            return true;
        }

    }

    public static class ReservationStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {
        private final ReservationManager reservationManager;

        public ReservationStage(@Nonnull final ReservationManager reservationManager) {
            this.reservationManager = Objects.requireNonNull(reservationManager);
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull final Map<Long, TopologyEntity.Builder> input) {
            final int numAdded = reservationManager.applyReservation(input,
                    this.getContext().getTopologyInfo());
            return Status.success("Added " + numAdded + " reserved entities to the topology.");
        }
    }

    /**
     * This stage is to update entity controllable flag.
     */
    public static class ControllableStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {
        private final ControllableManager controllableManager;

        public ControllableStage(@Nonnull final ControllableManager controllableManager) {
            this.controllableManager = Objects.requireNonNull(controllableManager);
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull final Map<Long, TopologyEntity.Builder> input) {
            final int controllableModified = controllableManager.applyControllable(input);
            final int suspendableModified = controllableManager.applySuspendable(input);
            final int resizeModified = controllableManager.applyScaleEligibility(input);
            final int resizeDownModified = controllableManager.applyResizeDownEligibility(input);
            return Status.success("Marked " + controllableModified + " entities as non-controllable. "
                    + "Marked " + suspendableModified + " entities as non-suspendable. "
                    + "Marked " + resizeModified + " entities as not resizeable. "
                    + "Marked " + resizeDownModified + " entities as not eligible for resize down.");
        }
    }

    /**
     * The ScopeResolutionStage will translate a list of scope entries (specified in the PlanScope)
     * into a list of "seed" entity oid's representing the focus on the topology to be analyzed.
     * These "seed entities" will be written into to the TopologyInfo for inclusion in the
     * broadcast.
     * <p>
     * Each scope entry will refer either to a specific entity, or to a group. Scope entry id's for
     * specific entities will be directly to the seed entity list. Scope entry id's for "Group"
     * types will be resolved to a list of group member entities, and each of those will be added
     * to the "seed entities" list.
     */
    public static class ScopeResolutionStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {
        private static final Logger logger = LogManager.getLogger();

        private final PlanScope planScope;

        private final GroupServiceBlockingStub groupServiceClient;

        public ScopeResolutionStage(@Nonnull GroupServiceBlockingStub groupServiceClient,
                                    @Nullable final PlanScope planScope) {
            this.groupServiceClient = groupServiceClient;
            this.planScope = planScope;
        }

        @NotNull
        @Override
        public Status passthrough(final TopologyGraph<TopologyEntity> input) throws PipelineStageException {
            // if no scope to apply, this function does nothing.
            if (planScope == null || planScope.getScopeEntriesCount() == 0) {
                return Status.success("No scope to apply.");
            }
            // translate the set of groups and entities in the plan scope into a list of "seed"
            // entities for the market to focus on. The list of "seeds" should include all seeds
            // already in the list, all entities individually specified in the scope, as well as all
            // entities belonging to groups that are in scope.

            // initialize the set with any existing seed oids
            final Set<Long> seedEntities = new HashSet<>(getContext().getTopologyInfo().getScopeSeedOidsList());
            final Set<Long> groupsToResolve = new HashSet<>();
            List<ScenarioOuterClass.PlanScopeEntry> planScopeEntries = planScope.getScopeEntriesList();
            for (ScenarioOuterClass.PlanScopeEntry scopeEntry : planScopeEntries) {
                // if it's an entity, add it right to the seed entity list. Otherwise queue it for
                // group resolution.
                if (StringConstants.GROUP_TYPES.contains(scopeEntry.getClassName())) {
                    groupsToResolve.add(scopeEntry.getScopeObjectOid());
                } else {
                    // this is an entity -- add it right to the seedEntities
                    seedEntities.add(scopeEntry.getScopeObjectOid());
                }
            }

            // Now resolve the groups by adding all the group members to the seedEntities
            if (groupsToResolve.size() > 0) {
                // fetch the group definitions from the group service
                GroupDTO.GetGroupsRequest request = GroupDTO.GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupDTO.GroupFilter.newBuilder()
                                .addAllId(groupsToResolve))
                        .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                        .build();
                groupServiceClient.getGroups(request).forEachRemaining(group -> {
                        try {
                            // add each group's members to the set of additional seed entities
                            Set<Long> groupMemberOids = getContext().getGroupResolver().resolve(group, input).getAllEntities();
                            seedEntities.addAll(groupMemberOids);
                        } catch (GroupResolutionException gre) {
                            // log a warning
                            logger.warn("Couldn't resolve members for group {} while defining scope", group);
                        }
                    }
                );
            }

            // Set scope type.
            // We have one scope entity type (e.g. REGION/BUSINESS ACCOUNT/AZ),
            // hence each of the planScopeEntries will have the same scope entity type.
            // For a mixed group of entities from different Regions/AZ/BFs, the scope will a group
            // of VMs/DB/DBS, for which resolution to relevant Regions/BA's/AZ will be made.
            // ResorceGroups will be handled similarly.
            // For Plan Account scope, the relevant BA's from same billing family will be used instead
            // of the scope oids for RI retrieval from the DB.
            // TODO:  Check with API if its possible to pass the scope type instead.
            String scopeClassName = planScopeEntries.get(0).getClassName();
            int scopeEntityType = (planScopeEntries.isEmpty() || scopeClassName == null)
                    ? CommonDTO.EntityDTO.EntityType.UNKNOWN_VALUE
                    : ApiEntityType.fromString(scopeClassName).typeNumber();
            // now add the new seed entities to the list, and the scope type for OCP plans.
            getContext().editTopologyInfo(topologyInfoBuilder -> {
                topologyInfoBuilder.clearScopeSeedOids();
                topologyInfoBuilder
                        .addAllScopeSeedOids(seedEntities);
                topologyInfoBuilder.setScopeEntityType(scopeEntityType);
            });

            return Status.success("Added " + seedEntities.size() +
                    " seed entities to topology info.");
        }
    }

    /**
     * This stage creates a {@link TopologyGraph<TopologyEntity>} out of the topology entities.
     */
    public static class GraphCreationStage extends Stage<Map<Long, TopologyEntity.Builder>, TopologyGraph<TopologyEntity>> {

        private GroupScopeResolver groupScopeResolver;

        GraphCreationStage() {
            this(null);
        }

        GraphCreationStage(@Nullable GroupScopeResolver groupScopeResolver) {
            this.groupScopeResolver = groupScopeResolver;
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<TopologyGraph<TopologyEntity>> execute(@NotNull @Nonnull final Map<Long, TopologyEntity.Builder> input) {
            final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(input);
            if (groupScopeResolver != null) {
                groupScopeResolver.setTopologyGraph(graph);
            }
            return StageResult.withResult(graph)
                .andStatus(Status.success());
        }
    }

    /**
     * Stage to apply changes to commodities values like used,peak etc.
     */
    public static class CommoditiesEditStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final List<ScenarioChange> changes;

        private final CommoditiesEditor commoditiesEditor;

        private final PlanScope scope;


        public CommoditiesEditStage(@Nonnull CommoditiesEditor commoditiesEditor,
                                    @Nonnull List<ScenarioChange> changes, @Nonnull PlanScope scope) {
            this.changes = Objects.requireNonNull(changes);
            this.commoditiesEditor = Objects.requireNonNull(commoditiesEditor);
            this.scope = Objects.requireNonNull(scope);
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull TopologyGraph<TopologyEntity> graph) throws PipelineStageException {
            commoditiesEditor.applyCommodityEdits(graph, changes, getContext().getTopologyInfo(),
                    scope);
            // TODO (roman, 23 Oct 2018): Add some information about number/type of modified commodities?
            return Status.success();
        }
    }

    /**
     * Stage to apply some constraint-specific changes like disabling of ignored commodities.
     */
    public static class IgnoreConstraintsStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final GroupResolver groupResolver;

        private final GroupServiceBlockingStub groupService;

        private final ConstraintsEditor constraintsEditor;

        private final List<ScenarioChange> changes;

        public IgnoreConstraintsStage(@Nonnull GroupResolver groupResolver,
                                      @Nonnull GroupServiceBlockingStub groupService, @Nonnull List<ScenarioChange> changes) {
            this.groupResolver = Objects.requireNonNull(groupResolver);
            this.groupService = Objects.requireNonNull(groupService);
            this.changes = Objects.requireNonNull(changes);
            constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
        }

        @NotNull
        @Override
        public Status passthrough(TopologyGraph<TopologyEntity> input) throws PipelineStageException {
            boolean isPressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(getContext().getTopologyInfo());
            try {
                constraintsEditor.editConstraints(input, changes, isPressurePlan);
            } catch (ConstraintsEditorException e) {
                throw new PipelineStageException(e);
            }
            // TODO (roman, 23 Oct 2018): Add some high-level information about modifications made.
            return Status.success();
        }

        @Override
        public boolean required() {
            return true;
        }
    }

    /**
     * This stage is responsible for setting the environment type of entities in the graph.
     *
     * This needs to happen after things like topology editing and stitching, but before
     * policy/setting application in case we we have environment-type-specific
     * groups/policies/settings policies.
     *
     * This stage modifies the entities in the input {@link TopologyGraph<TopologyEntity>}.
     */
    public static class EnvironmentTypeStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final EnvironmentTypeInjector environmentTypeInjector;

        public EnvironmentTypeStage(@Nonnull final EnvironmentTypeInjector environmentTypeInjector) {
            this.environmentTypeInjector = Objects.requireNonNull(environmentTypeInjector);
        }

        @NotNull
        @Nonnull
        @Override
        public Status passthrough(final TopologyGraph<TopologyEntity> input) {
            final InjectionSummary injectionSummary =
                    environmentTypeInjector.injectEnvironmentType(input);

            final StringBuilder statusBuilder = new StringBuilder();
            if (injectionSummary.getUnknownCount() > 0) {
                statusBuilder.append("Could not determine type for ")
                        .append(injectionSummary.getUnknownCount())
                        .append(" entities.\n");
            }
            if (injectionSummary.getConflictingTypeCount() > 0) {
                statusBuilder.append(injectionSummary.getConflictingTypeCount())
                        .append(" entities already have env type set.\n");
            }
            injectionSummary.getEnvTypeCounts().forEach((envType, count) ->
                    // Should look something like "5 ON_PREM entities"
                    statusBuilder.append(count).append(" ").append(envType).append(" entities.\n"));

            // Note: if conflicting type count > 0, we may want to consider return a "failed" status.
            if (injectionSummary.getUnknownCount() > 0 || injectionSummary.getConflictingTypeCount() > 0) {
                return TopologyPipeline.Status.withWarnings(statusBuilder.toString());
            } else {
                return TopologyPipeline.Status.success(statusBuilder.toString());
            }
        }

        @Override
        public boolean required() {
            return true;
        }
    }

    /**
     * This stage applies policies to a {@link TopologyGraph<TopologyEntity>}. This makes changes
     * to the commodities of entities in the {@link TopologyGraph<TopologyEntity>} to reflect the
     * applied policies.
     */
    public static class PolicyStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final PolicyManager policyManager;
        private final List<ScenarioChange> changes;

        public PolicyStage(@Nonnull final PolicyManager policyManager) {
            this(policyManager, Collections.emptyList());
        }

        public PolicyStage(@Nonnull final PolicyManager policyManager, List<ScenarioChange> changes) {
            this.policyManager = policyManager;
            this.changes = Objects.requireNonNull(changes);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean required() {
            return true;
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull final TopologyGraph<TopologyEntity> input) {
            final PolicyApplicator.Results applicationResults =
                    policyManager.applyPolicies(getContext(), input, changes);
            final StringJoiner statusMsg = new StringJoiner("\n")
                .setEmptyValue("No policies to apply.");
            final boolean errors = applicationResults.getErrors().size() > 0;
            if (errors) {
                statusMsg.add(applicationResults.getErrors().size()
                    + " policies encountered errors and failed to run!\n");
            }
            if (applicationResults.getInvalidPolicyCount() > 0) {
                statusMsg.add(applicationResults.getInvalidPolicyCount()
                    + " policies were invalid and got ignored!\n");
            }
            if (applicationResults.getAppliedCounts().size() > 0) {
                statusMsg.add("(policy type) : (num applied)");
                applicationResults.getAppliedCounts().forEach((policyType, numApplied) -> {
                    statusMsg.add(FormattedString.format("{} : {}", policyType, numApplied));
                    applicationResults.getAddedCommodityCounts(policyType).forEach((commType, numAdded) -> {
                        statusMsg.add(FormattedString.format("    Added {} {} commodities.", numAdded, commType));
                    });
                });
            }

            if (applicationResults.getTotalAddedCommodityCounts().size() > 0) {
                statusMsg.add("(commodity type) : (num commodities added)");
                applicationResults.getTotalAddedCommodityCounts().forEach((commType, numAddded) -> {
                    statusMsg.add(FormattedString.format("{} : {}", commType, numAddded));
                });
            }

            if (errors || applicationResults.getInvalidPolicyCount() > 0) {
                return Status.withWarnings(statusMsg.toString());
            } else {
                return Status.success(statusMsg.toString());
            }
        }
    }

    /**
     * This stage takes {@link TopologyGraph} as input and produces
     * {@link GraphWithSettings} with empty settings as output.
     */
    public static class DummySettingsResolutionStage extends
            Stage<TopologyGraph<TopologyEntity>, GraphWithSettings> {
        @Nonnull
        @Override
        public StageResult<GraphWithSettings> execute(@NotNull @Nonnull final TopologyGraph<TopologyEntity> input) {
            return StageResult.withResult(new GraphWithSettings(input, Collections.emptyMap(), Collections.emptyMap()))
                    .andStatus(Status.success());
        }
    }

    /**
     * This stages resolves per-entity settings in the {@link TopologyGraph<TopologyEntity>}.
     * It's responsible for determining which settings apply to which entities, performing
     * conflict resolution (when multiple setting policies apply to a single entity), and
     * applying setting overrides from plan scenarios.
     */
    public static class SettingsResolutionStage extends Stage<TopologyGraph<TopologyEntity>, GraphWithSettings> {
        private final Logger logger = LogManager.getLogger();

        private final SettingOverrides settingOverrides;

        private final EntitySettingsResolver entitySettingsResolver;
        private final ConsistentScalingManager consistentScalingManager;

        private SettingsResolutionStage(@Nonnull final EntitySettingsResolver entitySettingsResolver,
                                        @Nonnull final List<ScenarioChange> scenarioChanges, final ConsistentScalingManager consistentScalingManager) {
            this.entitySettingsResolver = entitySettingsResolver;
            this.settingOverrides = new SettingOverrides(scenarioChanges);
            this.consistentScalingManager = consistentScalingManager;
        }

        public static SettingsResolutionStage live(@Nonnull final EntitySettingsResolver entitySettingsResolver,
                                                   @Nonnull final ConsistentScalingManager consistentScalingManager) {
            return new SettingsResolutionStage(entitySettingsResolver, Collections.emptyList(), consistentScalingManager);
        }

        public static SettingsResolutionStage plan(
            @Nonnull final EntitySettingsResolver entitySettingsResolver,
            @Nonnull final List<ScenarioChange> scenarioChanges,
            @Nonnull final ConsistentScalingManager consistentScalingManager) {
            return new SettingsResolutionStage(entitySettingsResolver, scenarioChanges,
                    consistentScalingManager);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<GraphWithSettings> execute(@NotNull @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
            final GraphWithSettings graphWithSettings = entitySettingsResolver.resolveSettings(
                getContext().getGroupResolver(), topologyGraph,
                settingOverrides, getContext().getTopologyInfo(), consistentScalingManager,
                getContext().getSettingPolicyEditors());
            return StageResult.withResult(graphWithSettings)
                // TODO (roman, Oct 23 2018): Provide some information about number of
                // setting policies applied.
                .andStatus(Status.success());
        }
    }

    /**
     * This stage uploads settings resolved in {@link SettingsResolutionStage} to
     * the group component.
     */
    public static class SettingsUploadStage extends PassthroughStage<GraphWithSettings> {
        private final EntitySettingsResolver entitySettingsResolver;

        public SettingsUploadStage(@Nonnull final EntitySettingsResolver entitySettingsResolver) {
            this.entitySettingsResolver = entitySettingsResolver;
        }

        @NotNull
        @Override
        public Status passthrough(final GraphWithSettings input) throws PipelineStageException {
            // This method does a sync call to Group Component.
            // If GC has trouble, the topology broadcast would be delayed.
            entitySettingsResolver.sendEntitySettings(getContext().getTopologyInfo(),
                input.getEntitySettings(), input.getTopologyGraph());
            return Status.success();
        }

        @Override
        public boolean required() {
            // If settings upload fails, then the system won't be able to properly handle the
            // broadcast - and may recommend wrong actions.
            return true;
        }
    }

    /**
     * This stage applies settings resolved in {@link SettingsResolutionStage} to the
     * {@link TopologyGraph<TopologyEntity>} the settings got resolved on. For example, if "suspend" is disabled
     * for entity 10, this stage is responsible for making sure that the relevant property
     * in {@link TopologyEntityDTO} reflects that.
     */
    public static class SettingsApplicationStage extends PassthroughStage<GraphWithSettings> {
        private final EntitySettingsApplicator settingsApplicator;

        public SettingsApplicationStage(@Nonnull final EntitySettingsApplicator settingsApplicator) {
            this.settingsApplicator = settingsApplicator;
        }

        @NotNull
        @Override
        public Status passthrough(final GraphWithSettings input) throws PipelineStageException {
            settingsApplicator.applySettings(getContext().getTopologyInfo(), input);
            // TODO (roman, Oct 23 2018): Information about number of entities modified as part of
            // setting application.
            // TODO (Gary, Oct 10, 2019): Information about what entity types are not overridden in MoveApplicator
            return Status.success();
        }

        @Override
        public boolean required() {
            // If settings application fails, the broadcast topology will be unaffected by any
            // setting policies, and we will generate wrong actions.
            return true;
        }
    }

    /**
     * We generate flow commodities in here.
     */
    public static class FlowGenerationStage extends PassthroughStage<StitchingContext> {
        /**
         * The matrix.
         */
        private final @Nonnull MatrixInterface matrix;

        /**
         * Constructs the {@link FlowGenerationStage}.
         *
         * @param matrix The matrix.
         */
        FlowGenerationStage(final @Nonnull MatrixInterface matrix) {
            this.matrix = matrix;
        }

        @NotNull
        @Override
        @Nonnull
        public Status passthrough(StitchingContext input) throws PipelineStageException {
            // Don't generate flow commodities if there are no flows.
            if (input != null && !matrix.isEmpty()) {
                FlowCommoditiesGenerator commoditiesGenerator = new FlowCommoditiesGenerator(matrix);
                commoditiesGenerator.generateCommodities(input.getStitchingGraph());
            }
            return Status.success();
        }
    }

    /**
     * We update matrix with capacities here.
     */
    public static class MatrixUpdateStage extends PassthroughStage<GraphWithSettings> {
        /**
         * The matrix.
         */
        private final @Nonnull MatrixInterface matrix;

        /**
         * Constructs the {@link MatrixUpdateStage}.
         *
         * @param matrix The matrix.
         */
        MatrixUpdateStage(final @Nonnull MatrixInterface matrix) {
            this.matrix = matrix;
        }

        @NotNull
        @Override
        @Nonnull
        public Status passthrough(GraphWithSettings input) throws PipelineStageException {
            // Don't make changes if there are no flows.
            if (input != null && !matrix.isEmpty()) {
                FlowCommoditiesGenerator commoditiesGenerator = new FlowCommoditiesGenerator(matrix);
                commoditiesGenerator.setFlowCapacities(input.getTopologyGraph());
            }
            return Status.success();
        }
    }

    /**
     * This stage applies post-stitching operations to apply additional stitching operations to the topology
     * that operate in a context with settings available. For additional details {@see PostStitchingOperation}.
     * <p>
     * Post-stitching should be applied in the same pipelines as the main stitching phase.
     * {@see StitchingStage}.
     */
    public static class PostStitchingStage extends PassthroughStage<GraphWithSettings> {

        private final StitchingManager stitchingManager;

        public PostStitchingStage(@Nonnull final StitchingManager stitchingManager) {
            this.stitchingManager = stitchingManager;
        }

        @NotNull
        @Override
        public Status passthrough(final GraphWithSettings input) throws PipelineStageException {
            // Set up the post-stitching journal.
            final IStitchingJournal<StitchingEntity> mainJournal = getContext()
                .getStitchingJournalContainer()
                .getMainStitchingJournal()
                .orElse(StitchingJournalFactory.emptyStitchingJournalFactory().stitchingJournal(null));
            final IStitchingJournal<TopologyEntity> postStitchingJournal = mainJournal.childJournal(
                new TopologyEntitySemanticDiffer(mainJournal.getJournalOptions().getVerbosity()));
            getContext().getStitchingJournalContainer().setPostStitchingJournal(postStitchingJournal);

            stitchingManager.postStitch(input, postStitchingJournal,
                    getContext().getPostStitchingOperationsToSkip());

            if (postStitchingJournal.shouldDumpTopologyAfterPostStitching()) {
                postStitchingJournal.dumpTopology(input.getTopologyGraph().entities());
            }

            final StitchingMetrics main = mainJournal.getMetrics();
            final StitchingMetrics postStitching = postStitchingJournal.getMetrics();

            final String status = new StringBuilder()
                .append(postStitching.getTotalChangesetsGenerated() - main.getTotalChangesetsGenerated())
                    .append(" changesets generated\n")
                .append(postStitching.getTotalChangesetsIncluded() - main.getTotalChangesetsIncluded())
                    .append(" changesets included\n")
                .append(postStitching.getTotalEmptyChangests() - main.getTotalEmptyChangests())
                    .append(" empty changesets\n")
                .toString();

            return Status.success(status);
        }
    }

    /**
     * This stage validates all entities within a graph, and sets any illegal commodity values to
     * appropriate substitutes.
     * <p>
     * This stage should occur after post-stitching; if it happens before, some commodity values
     * may be erroneously substituted and some necessary post-stitching operations may not occur.
     */
    public static class EntityValidationStage extends PassthroughStage<GraphWithSettings> {
        private final EntityValidator entityValidator;
        private final boolean isPlan;

        /**
         * Construct the validatio stage.
         *
         * @param entityValidator validator instance
         * @param isPlan whether the pipeline is working on a plan or live broadcast
         */
        public EntityValidationStage(@Nonnull final EntityValidator entityValidator, boolean isPlan) {
            this.entityValidator = entityValidator;
            this.isPlan = isPlan;
        }

        @NotNull
        @Override
        public Status passthrough(final GraphWithSettings input) throws PipelineStageException {
            try {
                entityValidator.validateTopologyEntities(input.getTopologyGraph(), isPlan);
                return Status.success();
            } catch (EntitiesValidationException e) {
                throw new PipelineStageException(e);
            }

        }

        @Override
        public boolean required() {
            return true;
        }
    }

    /**
     * This stage validates all entities within a graph according to the supply chain definitions
     * provided by the probes.
     */
    public static class SupplyChainValidationStage extends PassthroughStage<GraphWithSettings> {
        private final SupplyChainValidator supplyChainValidator;

        public SupplyChainValidationStage(
            @Nonnull final SupplyChainValidator supplyChainValidator) {
            this.supplyChainValidator = supplyChainValidator;
        }

        @NotNull
        @Override
        public Status passthrough(final GraphWithSettings input) throws PipelineStageException {
            final List<SupplyChainValidationFailure> validationErrors =
                    supplyChainValidator.validateTopologyEntities(input.getTopologyGraph().entities());
            final Map<String, Long> errorsByType = validationErrors.stream()
                .collect(Collectors.groupingBy(error -> error.getClass().getSimpleName(), Collectors.counting()));

            if (errorsByType.isEmpty()) {
                return Status.success("No supply chain validation errors.");
            } else {
                return Status.withWarnings("Supply chain validation error counts:\n" +
                        errorsByType.entrySet().stream()
                                .map(entry -> entry.getKey() + " : " + entry.getValue())
                                .collect(Collectors.joining("\n")));
            }
        }

        @Override
        public boolean required() {
            return true;
        }
    }

    /**
     * Placeholder stage to extract the {@link TopologyGraph<TopologyEntity>} for the {@link BroadcastStage}
     * in the live and plan (but not plan-over-plan) topology.
     * <p>
     * Also records {@link TopologyInfo} to the {@link StitchingJournal}.
     * <p>
     * We shouldn't need this once plan-over-plan supports policies and settings.
     */
    public static class ExtractTopologyGraphStage extends Stage<GraphWithSettings, TopologyGraph<TopologyEntity>> {
        @NotNull
        @Nonnull
        @Override
        public StageResult<TopologyGraph<TopologyEntity>> execute(@NotNull @Nonnull final GraphWithSettings graphWithSettings)
                throws PipelineStageException, InterruptedException {
            return StageResult.withResult(graphWithSettings.getTopologyGraph())
                    .andStatus(Status.success());
        }
    }

    /**
     * This stage sorts the entities in the topology graph so that inter-entity references always
     * point to entities that appear earlier in the broadcast (e.g. sellers before consumers.
     */
    public static class TopSortStage extends Stage<TopologyGraph<TopologyEntity>, Stream<TopologyEntity>> {

        @NotNull
        @Nonnull
        @Override
        public StageResult<Stream<TopologyEntity>> execute(@NotNull @Nonnull final TopologyGraph<TopologyEntity> graph)
                throws PipelineStageException, InterruptedException {
            return StageResult.withResult(graph.topSort()).andStatus(Status.success());
        }
    }

    /**
     * This stage broadcasts the topology, presented as a stream of {@link TopologyEntity instances},
     * out of the topology processor.
     */
    public static class BroadcastStage extends Stage<Stream<TopologyEntity>, TopologyBroadcastInfo> {

        private final Logger logger = LogManager.getLogger();

        private final List<TopoBroadcastManager> broadcastManagers;

        private final Map<ApiEntityType, EntityCounter> counts = new HashMap<>();

        private final MatrixInterface matrix;

        /**
         * Constructs the BroadcastStage.
         *
         * @param broadcastManagers The brodcast managers.
         * @param matrix            The matrix.
         */
        public BroadcastStage(@Nonnull final List<TopoBroadcastManager> broadcastManagers,
                final MatrixInterface matrix) {
            this.broadcastManagers = Objects.requireNonNull(broadcastManagers);
            this.matrix = Objects.requireNonNull(matrix);
            Preconditions.checkArgument(broadcastManagers.size() > 0);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<TopologyBroadcastInfo> execute(@NotNull @Nonnull final Stream<TopologyEntity> input)
            throws PipelineStageException, InterruptedException {

            // Record TopologyInfo and Metrics to the journal if there is one.
            getContext().getStitchingJournalContainer().getPostStitchingJournal().ifPresent(journal -> {
                journal.recordTopologyInfoAndMetrics(getContext().getTopologyInfo(), journal.getMetrics());
                journal.flushRecorders();
            });

            // The number of entities that have errors.
            // Note that this will only be "valid" after the iterator is consumed.
            final MutableInt numEntitiesWithErrors = new MutableInt(0);
            final Iterator<TopologyEntityDTO> entities = input
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .map(TopologyEntityDTO.Builder::build)
                    .peek(entity ->  {
                        if (!StitchingErrors.fromProtobuf(entity).isNone()) {
                            // Incrementing the error count as a side-effect of the map is not ideal,
                            // but we don't want to take another pass through entities just to count
                            // the ones with errors.
                            numEntitiesWithErrors.increment();
                        }
                    })
                    .iterator();
            try {
                final TopologyBroadcastInfo broadcastInfo;
                switch (getContext().getTopologyInfo().getTopologyType()) {
                    case REALTIME:
                        broadcastInfo = broadcastTopology(broadcastManagers.stream()
                            .<Function<TopologyInfo, TopologyBroadcast>>map(manager -> manager::broadcastLiveTopology)
                            .collect(Collectors.toList()), entities);
                        break;
                    case PLAN:
                        broadcastInfo = broadcastTopology(broadcastManagers.stream()
                            .<Function<TopologyInfo, TopologyBroadcast>>map(manager -> manager::broadcastUserPlanTopology)
                            .collect(Collectors.toList()), entities);
                        break;
                    default:
                        throw new IllegalStateException();
                }
                if (broadcastInfo.getEntityCount() == 0) {
                    return StageResult.withResult(broadcastInfo)
                        .andStatus(Status.withWarnings("No entities broadcast."));
                } else {
                    StringJoiner stringJoiner = new StringJoiner("\n");
                    stringJoiner.add("Broadcast " + broadcastInfo.getEntityCount() +
                        " entities [" + FileUtils.byteCountToDisplaySize(
                            broadcastInfo.getSerializedTopologySizeBytes()) + "]");
                    counts.forEach((type, count) -> stringJoiner
                        .add(count.getEntityCount() + " of type " + type.apiStr()
                            + " [" + count.getHumanReadableSize() + "]"));

                    if (numEntitiesWithErrors.intValue() > 0) {
                        // There were some entities with stitching (or other pipeline) errors.
                        stringJoiner.add("=============================");
                        stringJoiner.add(numEntitiesWithErrors + " of them had pipeline errors.");
                        return StageResult.withResult(broadcastInfo)
                            .andStatus(Status.withWarnings(stringJoiner.toString()));
                    } else {
                        return StageResult.withResult(broadcastInfo)
                            .andStatus(Status.success(stringJoiner.toString()));
                    }
                }
            } catch (CommunicationException e) {
                throw new PipelineStageException(e);
            }
        }

        private TopologyBroadcastInfo broadcastTopology(
            @Nonnull List<Function<TopologyInfo, TopologyBroadcast>> broadcastFunctions,
            final Iterator<TopologyEntityDTO> topology)
            throws InterruptedException, CommunicationException {
            final TopologyInfo topologyInfo = getContext().getTopologyInfo();
            final List<TopologyBroadcast> broadcasts = broadcastFunctions.stream()
                .map(function -> function.apply(topologyInfo))
                .collect(Collectors.toList());
            for (Iterator<TopologyEntityDTO> it = topology; it.hasNext(); ) {
                final TopologyEntityDTO entity = it.next();
                for (TopologyBroadcast broadcast : broadcasts) {
                    try {
                        broadcast.append(entity);
                    } catch (OversizedElementException e) {
                        logger.error("Entity {} (name: {}, type: {}) failed to be" +
                            " broadcast because it's too large: {}", entity.getOid(),
                            entity.getDisplayName(), entity.getEntityType(), e.getMessage());
                    }
                }
                counts.computeIfAbsent(ApiEntityType.fromType(entity.getEntityType()),
                    k -> new EntityCounter()).count(entity);
            }
            // Export Matrix.
            matrix.exportMatrix(new MatrixExporter(broadcasts));
            //
            // TODO(MB): Add action merging/conversion extension.
            //
            // Do the accounting.
            for (TopologyBroadcast broadcast : broadcasts) {
                final long sentCount = broadcast.finish();
                logger.info("Successfully sent {} entities within topology {} for context {} " +
                        "and broadcast of type {}. Type breakdown: {}", sentCount, topologyInfo.getTopologyId(),
                    topologyInfo.getTopologyContextId(), broadcast.getClass().getSimpleName(), counts);
            }

            return new TopologyBroadcastInfo(
                counts.values().stream().mapToLong(EntityCounter::getEntityCount).sum(),
                topologyInfo.getTopologyId(),
                topologyInfo.getTopologyContextId(),
                counts.values().stream().mapToLong(EntityCounter::getCumulativeSizeBytes).sum());
        }
    }

    /**
     * The stage to override the usage of workload and its provider based on user configurations.
     */
    public static class OverrideWorkLoadDemandStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final DemandOverriddenCommodityEditor demandOverriddenCommodityEditor;
        private final SearchResolver<TopologyEntity> searchResolver;
        private final GroupServiceBlockingStub groupServiceClient;
        private final List<ScenarioChange> changes;
        private final GroupResolverSearchFilterResolver searchFilterResolver;

        public OverrideWorkLoadDemandStage(@Nonnull final DemandOverriddenCommodityEditor demandOverriddenCommodityEditor,
                                           @Nonnull final SearchResolver<TopologyEntity> searchResolver,
                                           @Nonnull final GroupServiceBlockingStub groupServiceClient,
                                           @Nonnull final List<ScenarioChange> changes,
                                           @Nonnull final GroupResolverSearchFilterResolver searchFilterResolver) {
            this.demandOverriddenCommodityEditor = Objects.requireNonNull(demandOverriddenCommodityEditor);
            this.searchResolver = searchResolver;
            this.groupServiceClient = groupServiceClient;
            this.changes = Objects.requireNonNull(changes);
            this.searchFilterResolver = Objects.requireNonNull(searchFilterResolver);
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull TopologyGraph<TopologyEntity> graph) throws PipelineStageException {
            TopologyInfo topoInfo = getContext().getTopologyInfo();
            if (!topoInfo.hasPlanInfo()) { // skip non plan cases
                return Status.success();
            }
            final GroupResolver groupResolver = new GroupResolver(searchResolver, groupServiceClient,
                    searchFilterResolver);
            try {
                demandOverriddenCommodityEditor.applyDemandUsageChange(graph, groupResolver, changes);
            } catch (GroupResolutionException e) {
                throw new PipelineStageException(e);
            }
            return Status.success();
        }
    }

    /**
     * Stage to reduce the entities by plan scope.
     */
    public static class PlanScopingStage extends Stage<TopologyGraph<TopologyEntity>, TopologyGraph<TopologyEntity>> {

        private final PlanScope planScope;
        private final PlanTopologyScopeEditor planTopologyScopeEditor;
        private final SearchResolver<TopologyEntity> searchResolver;
        private final GroupServiceBlockingStub groupServiceClient;
        private final List<ScenarioChange> changes;
        private final GroupResolverSearchFilterResolver searchFilterResolver;

        public PlanScopingStage(@Nonnull final PlanTopologyScopeEditor topologyScopeEditor,
                                @Nullable final PlanScope planScope,
                                @Nonnull final SearchResolver<TopologyEntity> searchResolver,
                                @Nonnull final List<ScenarioChange> changes,
                                @Nullable final GroupServiceBlockingStub groupServiceClient,
                                @Nonnull final GroupResolverSearchFilterResolver searchFilterResolver) {
            this.planScope = planScope;
            this.planTopologyScopeEditor = Objects.requireNonNull(topologyScopeEditor);
            this.searchResolver = Objects.requireNonNull(searchResolver);
            this.groupServiceClient = groupServiceClient;
            this.changes = changes;
            this.searchFilterResolver = Objects.requireNonNull(searchFilterResolver);
        }

        @NotNull
        @Override
        public StageResult<TopologyGraph<TopologyEntity>> execute(@NotNull @Nonnull final TopologyGraph<TopologyEntity> graph)
                throws PipelineStageException, InterruptedException {
            if (planScope == null || planScope.getScopeEntriesList().isEmpty()) {
                return StageResult.withResult(graph).andStatus(Status.success());
            }
            TopologyInfo topologyInfo = getContext().getTopologyInfo();
            TopologyGraph<TopologyEntity> result;
            if (!topologyInfo.hasPlanInfo()) {
                throw new PipelineStageException("Plan with topology context id " +
                        topologyInfo.getTopologyContextId() + " has no planInfo object");
            }
            final GroupResolver groupResolver = new GroupResolver(searchResolver, groupServiceClient,
                    searchFilterResolver);
            if (!topologyInfo.getPlanInfo().getPlanType().equals(StringConstants.OPTIMIZE_CLOUD_PLAN)
                            && !topologyInfo.getPlanInfo().getPlanType().equals(StringConstants.CLOUD_MIGRATION_PLAN)) {
                // populate InvertedIndex
                logger.info("Indexing on-prem entities for scoping .....");
                InvertedIndex<TopologyEntity, TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider>
                        index = planTopologyScopeEditor.createInvertedIndex();
                graph.entities().forEach(entity -> index.add(entity));
                PlanProjectType planProjectType = topologyInfo.getPlanInfo().getPlanProjectType();
                // scope using inverted index
                try {
                    result = planTopologyScopeEditor.indexBasedScoping(index, graph, groupResolver, planScope, planProjectType);
                } catch (GroupResolutionException e) {
                    throw new PipelineStageException(e);
                }
                return StageResult.withResult(result).andStatus(Status.success("PlanScopingStage:"
                                + " Constructed a scoped topology of size " + result.size() +
                                " from topology of size " + graph.size()));
            } else {
                // cloud plans
                result = planTopologyScopeEditor.scopeTopology(topologyInfo, graph, getContext());
                return StageResult.withResult(result)
                                .andStatus(Status.success("PlanScopingStage: Constructed a scoped topology of size "
                                                + result.size() + " from topology of size " + graph.size()));
            }
        }

    }

    /**
     * Stage to apply changes to commodities values like used,peak etc.
     */
    public static class HistoricalUtilizationStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {

        private final HistoricalEditor historicalEditor;

        private final List<ScenarioChange> changes;

        public HistoricalUtilizationStage(@Nonnull final HistoricalEditor historicalEditor) {
            this(historicalEditor, Collections.emptyList());
        }

        public HistoricalUtilizationStage(@Nonnull final HistoricalEditor historicalEditor,
                                          @Nonnull final List<ScenarioChange> changes) {
            this.historicalEditor = Objects.requireNonNull(historicalEditor);
            this.changes = Objects.requireNonNull(changes);
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull TopologyGraph<TopologyEntity> graph) throws PipelineStageException {
            historicalEditor.applyCommodityEdits(graph, changes, getContext().getTopologyInfo());
            return Status.success();
        }
    }

    /**
     * Stages to copy historical values onto ephemeral entities (ie Containers) from
     * their associated persistent entities (ie ContainerSpec)
     */
    public static class EphemeralEntityHistoryStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {
        private final EphemeralEntityEditor ephemeralEntityEditor;

        /**
         * Create a new {@link EphemeralEntityHistoryStage}.
         * @param ephemeralEntityEditor The {@link EphemeralEntityEditor} to be run in this stage.
         */
        public EphemeralEntityHistoryStage(@Nonnull final EphemeralEntityEditor ephemeralEntityEditor) {
            this.ephemeralEntityEditor = Objects.requireNonNull(ephemeralEntityEditor);
        }

        @NotNull
        @Override
        @Nonnull
        public Status passthrough(@Nonnull TopologyGraph<TopologyEntity> graph) throws PipelineStageException {
            final EditSummary editSummary = ephemeralEntityEditor.applyEdits(graph);

            final StringBuilder statusBuilder = new StringBuilder();
            if (editSummary.getCommoditiesAdjusted() > 0) {
                statusBuilder.append("Adjusted ")
                    .append(editSummary.getCommoditiesAdjusted())
                    .append(" commodities.\n");
            }
            if (editSummary.getCommoditiesWithInsufficientData() > 0) {
                statusBuilder.append("Disabled resize on ")
                    .append(editSummary.getCommoditiesWithInsufficientData())
                    .append(" commodities because of insufficient data.\n");
            }
            if (editSummary.getInconsistentScalingGroups() > 0) {
                statusBuilder.append("Disabled resize on ")
                    .append(editSummary.getInconsistentScalingGroups())
                    .append(" consistent scaling groups (of ")
                    .append(editSummary.getTotalScalingGroups())
                    .append(" total) due to inconsistent capacities.");
            }

            // We want to warn when there are inconsistent scaling groups. Ideally we want to
            // actually still be able to scale but today we don't know how to.
            if (editSummary.getInconsistentScalingGroups() > 0) {
                return TopologyPipeline.Status.withWarnings(statusBuilder.toString());
            } else {
                return TopologyPipeline.Status.success(statusBuilder.toString());
            }
        }
    }

    /**
     * Handles some cloud migration specific tasks.
     */
    public static class CloudMigrationPlanStage extends
            Stage<TopologyGraph<TopologyEntity>, TopologyGraph<TopologyEntity>> {
        /**
         * Plan scoping info.
         */
        private final PlanScope planScope;

        /**
         * Helper doing the real migration stage work.
         */
        private final CloudMigrationPlanHelper cloudMigrationPlanHelper;

        /**
         * User specified scenario changes.
         */
        private final List<ScenarioChange> changes;

        /**
         * Creates new stage.
         *
         * @param cloudMigrationPlanHelper Helper for migration stage.
         * @param planScope Scope for plan.
         * @param changes Scenario changes.
         */
        public CloudMigrationPlanStage(
                @Nonnull final CloudMigrationPlanHelper cloudMigrationPlanHelper,
                @Nullable final PlanScope planScope,
                @Nonnull final List<ScenarioChange> changes) {
            this.cloudMigrationPlanHelper = Objects.requireNonNull(cloudMigrationPlanHelper);
            this.planScope = planScope;
            this.changes = changes;
        }

        /**
         * Runs the migration stage.
         *
         * @param inputGraph Input graph from previous pipeline stage.
         * @return Updated graph going to next stage of pipeline.
         * @throws PipelineStageException Thrown on stage processing issues.
         */
        @Override
        @Nonnull
        public StageResult<TopologyGraph<TopologyEntity>> execute(
                @Nonnull final TopologyGraph<TopologyEntity> inputGraph)
                throws PipelineStageException {
            TopologyGraph<TopologyEntity> outputGraph = cloudMigrationPlanHelper.executeStage(
                    getContext(), inputGraph, planScope, changes);
            return StageResult.withResult(outputGraph)
                    .andStatus(Status.success("CloudMigrationPlanStage: Output topology of size "
                            + outputGraph.size() + ", from input topology of size "
                            + inputGraph.size()));
        }
    }

    /**
     * Stage to inject min and max thresholds to Container entity request commodities.
     * These thresholds control the behavior of resize analysis for these commodities in the market.
     */
    public static class RequestCommodityThresholdsStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {
        private final RequestAndLimitCommodityThresholdsInjector requestAndLimitCommodityThresholdsInjector;

        /**
         * Create a new {@link RequestCommodityThresholdsStage}.
         * @param requestAndLimitCommodityThresholdsInjector The {@link RequestAndLimitCommodityThresholdsInjector} to
         *                                           be run in this stage.
         */
        public RequestCommodityThresholdsStage(
            @Nonnull final RequestAndLimitCommodityThresholdsInjector requestAndLimitCommodityThresholdsInjector) {
            this.requestAndLimitCommodityThresholdsInjector = Objects.requireNonNull(requestAndLimitCommodityThresholdsInjector);
        }

        @NotNull
        @Override
        @Nonnull
        public Status passthrough(@Nonnull TopologyGraph<TopologyEntity> graph) throws PipelineStageException {
            requestAndLimitCommodityThresholdsInjector.injectThresholds(graph);
            return Status.success();
        }
    }

    /**
     * Stage to apply changes to properties based on action capabilities.
     */
    public static class ProbeActionCapabilitiesApplicatorStage extends PassthroughStage<TopologyGraph<TopologyEntity>> {
        private final ProbeActionCapabilitiesApplicatorEditor probeActionCapabilitiesApplicatorEditor;

        public ProbeActionCapabilitiesApplicatorStage(@Nonnull ProbeActionCapabilitiesApplicatorEditor editor) {
            this.probeActionCapabilitiesApplicatorEditor = editor;
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull TopologyGraph<TopologyEntity> graph) {
            final EditorSummary editorSummary = probeActionCapabilitiesApplicatorEditor
                .applyPropertiesEdits(graph);
            final String statusSummary =
                "Total movables set to true are: " + editorSummary.getMovableToTrueCounter() +
                "\nTotal movables set to false are: " + editorSummary.getMovableToFalseCounter() +
                        "\nTotal resizeables set to true are: " + editorSummary.getResizeableToTrueCounter() +
                        "\nTotal resizeables set to false are: " + editorSummary.getResizeableToFalseCounter() +
                        "\nTotal scalables set to true are: " + editorSummary.getScalableToTrueCounter() +
                        "\nTotal scalables set to false are: " + editorSummary.getScalableToFalseCounter() +
                "\nTotal cloneable set to true are: " + editorSummary.getCloneableToTrueCounter() +
                "\nTotal cloneable set to false are: " + editorSummary.getCloneableToFalseCounter() +
                "\nTotal suspendable set to true are: " + editorSummary.getSuspendableToTrueCounter() +
                "\nTotal suspendable set to false are: " + editorSummary.getSuspendableToFalseCounter();
            return Status.success(statusSummary);
        }
    }

    /**
     * Dummy wrapper around class that does the work of applying
     * different kinds of history values calculations to commodities.
     * Added for compliance.
     */
    public static class HistoryAggregationStage extends PassthroughStage<GraphWithSettings> {
        private final HistoryAggregator historyAggregator;
        private final List<ScenarioChange> changes;
        private final TopologyDTO.TopologyInfo topologyInfo;
        private final PlanScope scope;

        /**
         * Construct the stage wrapper.
         *
         * @param historyAggregator historical values calculator
         * @param changes plan scenarios, if applicable
         * @param topologyInfo topology information
         * @param scope plan scope, if applicable
         */
        public HistoryAggregationStage(@Nonnull HistoryAggregator historyAggregator,
                                              @Nullable List<ScenarioChange> changes,
                                              @Nonnull TopologyDTO.TopologyInfo topologyInfo,
                                              @Nullable PlanScope scope) {
            this.historyAggregator = historyAggregator;
            this.changes = changes;
            this.topologyInfo = topologyInfo;
            this.scope = scope;
        }

        @NotNull
        @Override
        public Status passthrough(@Nonnull GraphWithSettings graph) throws PipelineStageException {
            historyAggregator.applyCommodityEdits(graph, changes, topologyInfo, scope);
            return Status.success();
        }
    }

    /**
     * The Communication Matrix Exporter.
     */
    private static class MatrixExporter implements MatrixInterface.Codec {
        /**
         * The current component.
         */
        private MatrixInterface.Component component_;

        /**
         * The Matrix builder.
         */
        private TopologyDTO.TopologyExtension.Matrix.Builder matrixBuilder_;

        /**
         * The topology broadcasters.
         */
        private final List<TopologyBroadcast> broadcasts_;

        /**
         * Constructs the matrix exporter.
         *
         * @param broadcasts The topology broadcasters.
         */
        MatrixExporter(final @Nonnull List<TopologyBroadcast> broadcasts) {
            broadcasts_ = broadcasts;
        }

        /**
         * Start the next component.
         *
         * @param component The matrix component
         * @throws IllegalStateException Never.
         */
        @Override public void start(final @Nonnull MatrixInterface.Component component)
            throws IllegalStateException {
            component_ = component;
            if (matrixBuilder_ == null) {
                matrixBuilder_ = TopologyDTO.TopologyExtension.Matrix.newBuilder();
            }
        }

        /**
         * Processes the next message for the current component.
         *
         * @param msg The message.
         * @param <T> The message type.
         * @throws IllegalStateException If the processing fails down the stream.
         */
        @Override
        public <T extends AbstractMessage> void next(@Nonnull T msg)
            throws IllegalStateException {
            switch (component_) {
                case OVERLAY:
                    matrixBuilder_.addEdges((MatrixDTO.OverlayEdge)msg);
                    break;
                case UNDERLAY:
                    matrixBuilder_.addUnderlay((MatrixDTO.UnderlayStruct)msg);
                    break;
                case CONSUMER_2_PROVIDER:
                    matrixBuilder_.addConsumerToProvider((MatrixDTO.ConsumerToProvider)msg);
                    break;
            }
            sendChunk(MessageChunker.CHUNK_SIZE);
        }

        /**
         * Sends the next chunk.
         *
         * @param threshold The minimum number of elements in the matrix needed to send things.
         */
        private void sendChunk(final int threshold) {
            if (matrixBuilder_.getEdgesCount() + matrixBuilder_.getUnderlayCount() +
                matrixBuilder_.getConsumerToProviderCount() <= threshold) {
                return;
            }
            TopologyDTO.TopologyExtension ext = TopologyDTO.TopologyExtension
                                                    .newBuilder().setMatrix(matrixBuilder_.build())
                                                    .build();
            // Send the next chunk
            for (TopologyBroadcast broadcast : broadcasts_) {
                try {
                    broadcast.appendExtension(ext);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            matrixBuilder_.clear();
        }

        /**
         * Finishes current component.
         *
         * @throws IllegalStateException Never.
         */
        @Override
        public void finish() throws IllegalStateException {
            sendChunk(0);
            component_ = null;
        }
    }

    /**
     * A counter to keep track of the number of entities together with their size.
     */
    static class EntityCounter {
        /**
         * The number of entities.
         */
        private long entityCount;

        /**
         * The cumulative size of the entities counted.
         */
        private long cumulativeSizeBytes;

        public EntityCounter() {
            entityCount = 0;
            cumulativeSizeBytes = 0;
        }

        public void count(@Nonnull final TopologyEntityDTO entity) {
            entityCount++;
            cumulativeSizeBytes += entity.getSerializedSize();
        }

        /**
         * Get the number of entities counted by this counter.
         *
         * @return the number of entities counted by this counter.
         */
        public long getEntityCount() {
            return entityCount;
        }

        /**
         * Get the cumulative (serialized) size in bytes of the entities counted by this counter.
         *
         * @return the cumulative (serialized) size in bytes of the entities counted by this counter.
         */
        public long getCumulativeSizeBytes() {
            return cumulativeSizeBytes;
        }

        /**
         * Get the cumulative (serialized) size in bytes of the entities counted by this counter
         * formatted in a human readable format.
         *
         * @return The size in bytes in a human readable format.
         */
        public String getHumanReadableSize() {
            return FileUtils.byteCountToDisplaySize(cumulativeSizeBytes);
        }

        @Override
        public String toString() {
            return Long.toString(entityCount) + "[" + getHumanReadableSize() + "]";
        }
    }
}
