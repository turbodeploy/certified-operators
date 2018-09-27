package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.TopologyEntitySemanticDiffer;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.controllable.ControllableManager;
import com.vmturbo.topology.processor.entity.EntitiesValidationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.group.settings.SettingOverrides;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.reservation.ReservationManager;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidator;
import com.vmturbo.topology.processor.topology.CommoditiesEditor;
import com.vmturbo.topology.processor.topology.ConstraintsEditor;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * A wrapper class for the various {@link Stage} and {@link PassthroughStage} implementations.
 * Since the stages are pretty small, it makes some sense to keep them in one place for now.
 */
public class Stages {

    /**
     * This stage uploads discovered groups (and clusters) to the group component.
     * We only do this for the live topology.
     */
    public static class UploadGroupsStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {

        private final DiscoveredGroupUploader discoveredGroupUploader;

        public UploadGroupsStage(@Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
            this.discoveredGroupUploader = discoveredGroupUploader;
        }

        @Nonnull
        @Override
        public void passthrough(final Map<Long, TopologyEntity.Builder> input) {
            discoveredGroupUploader.uploadDiscoveredGroups(input);
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

        @Nonnull
        @Override
        public void passthrough(final Map<Long, TopologyEntity.Builder> input) {
            discoveredWorkflowUploader.uploadDiscoveredWorkflows();
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

        @Override
        public void passthrough(final Map<Long, TopologyEntity.Builder> input) throws PipelineStageException {
            try {
                notifier.sendTemplateDeploymentProfileData();
            } catch (CommunicationException e) {
                throw new PipelineStageException(e);
            }
        }
    }

    /**
     * This stage stitches together the various sub-topologies discovered by individual targets
     * into a unified "complete" topology.
     *
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

        @Nonnull
        @Override
        public StitchingContext execute(@Nonnull final EntityStore entityStore) {
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
            return stitchingManager.stitch(stitchingContext, journal);
        }
    }

    /**
     * A stage that fixes up groups that references entities whose identifiers were modified
     * during stitching. See {@link StitchingGroupFixer} for more details.
     *
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

        @Override
        public void passthrough(StitchingContext input) throws PipelineStageException {
            stitchingGroupFixer.fixupGroups(input.getStitchingGraph(),
                discoveredGroupUploader.buildMemberCache());
        }
    }

    /**
     * This stage scans service entities for discovered setting policies to be uploaded.
     * See {@link com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage}.
     *
     * This stage only happens in the live topology broadcast.
     */
    public static class ScanDiscoveredSettingPoliciesStage
        extends Stage<StitchingContext, Map<Long, TopologyEntity.Builder>> {

        private static final Logger logger = LogManager.getLogger();

        private final DiscoveredGroupUploader discoveredGroupUploader;
        private final DiscoveredSettingPolicyScanner settingPolicyScanner;

        public ScanDiscoveredSettingPoliciesStage(@Nonnull final DiscoveredSettingPolicyScanner scanner,
                                                  @Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
            this.settingPolicyScanner = Objects.requireNonNull(scanner);
            this.discoveredGroupUploader = Objects.requireNonNull(discoveredGroupUploader);
        }

        @Nonnull
        @Override
        public Map<Long, TopologyEntity.Builder> execute(@Nonnull final StitchingContext stitchingContext) {
            try {
                settingPolicyScanner.scanForDiscoveredSettingPolicies(stitchingContext,
                    discoveredGroupUploader);
            } catch (RuntimeException e) {
                // This stage is not required, but it's not quite a passthrough either. If an
                // exception occurs, log a warning and continue.
                logger.warn("Non-required pipeline stage {} failed with error: {}",
                    getClass().getSimpleName(), e.getMessage());
            }

            return stitchingContext.constructTopology();
        }
    }

    /**
     * This stage is for adding cluster commodities to relate TopologyEntityDTO.
     */
    public static class ApplyClusterCommodityStage extends PassthroughStage<TopologyGraph> {
        private final DiscoveredClusterConstraintCache discoveredClusterConstraintCache;

        public ApplyClusterCommodityStage(
                @Nonnull final DiscoveredClusterConstraintCache discoveredClusterConstraintCache) {
            this.discoveredClusterConstraintCache = discoveredClusterConstraintCache;
        }

        @Nonnull
        @Override
        public void passthrough(@Nonnull final TopologyGraph topologyGraph) {
            discoveredClusterConstraintCache.applyClusterCommodity(topologyGraph);
        }
    }

    /**
     * This stage construct a topology map (ie OID -> TopologyEntity.Builder) from a {@Link StitchingContext}.
     */
    public static class ConstructTopologyFromStitchingContextStage
        extends Stage<StitchingContext, Map<Long, TopologyEntity.Builder>> {

        public ConstructTopologyFromStitchingContextStage() {

        }

        @Nonnull
        @Override
        public Map<Long, TopologyEntity.Builder> execute(@Nonnull final StitchingContext stitchingContext) {
            return stitchingContext.constructTopology();
        }
    }

    /**
     * This stage acquires an old topology from the repository.
     *
     * We only do this in plans, because the live topology pipeline doesn't depend
     * on old topologies.
     */
    public static class TopologyAcquisitionStage extends Stage<Long, Map<Long, TopologyEntity.Builder>> {

        private final RepositoryClient repository;

        public TopologyAcquisitionStage(@Nonnull final RepositoryClient repositoryClient) {
            this.repository = Objects.requireNonNull(repositoryClient);
        }

        @Nonnull
        @Override
        public Map<Long, TopologyEntity.Builder> execute(@Nonnull final Long topologyId) {
            // we need to gather the entire topology in order to perform editing below
            Iterable<RepositoryDTO.RetrieveTopologyResponse> dtos =
                    () -> repository.retrieveTopology(topologyId);
            return StreamSupport.stream(dtos.spliterator(), false)
                    .map(RepositoryDTO.RetrieveTopologyResponse::getEntitiesList)
                    .flatMap(List::stream)
                    .map(TopologyEntityDTO::toBuilder)
                    .collect(Collectors.toMap(TopologyEntityDTOOrBuilder::getOid,
                        // TODO: Persist and pass through discovery information for this entity.
                        TopologyEntity::newBuilder));
        }
    }

    /**
     * This stage applies a set of {@link ScenarioChange} objects to a topology.
     *
     * We only do this in plans because only plans have scenario changes on top of a topology.
     */
    public static class TopologyEditStage extends PassthroughStage<Map<Long, TopologyEntity.Builder>> {

        private final TopologyEditor topologyEditor;
        private final List<ScenarioChange> changes;

        public TopologyEditStage(@Nonnull final TopologyEditor topologyEditor,
                                 @Nonnull final List<ScenarioChange> scenarioChanges) {
            this.topologyEditor = Objects.requireNonNull(topologyEditor);
            this.changes = Objects.requireNonNull(scenarioChanges);
        }

        @Override
        public void passthrough(@Nonnull final Map<Long, TopologyEntity.Builder> input) {
            // Topology editing should use a group resolver distinct from the rest of the pipeline.
            // This is so that pre-edit group membership lookups don't get cached in the "main"
            // group resolver, preventing post-edit group membership lookups from seeing members
            // added or removed during editing.
            final GroupResolver groupResolver = new GroupResolver(new TopologyFilterFactory());
            topologyEditor.editTopology(input, changes, getContext().getTopologyInfo(), groupResolver);
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

        @Override
        public void passthrough(@Nonnull final Map<Long, TopologyEntity.Builder> input) {
            reservationManager.applyReservation(input);
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

        @Override
        public void passthrough(@Nonnull final Map<Long, TopologyEntity.Builder> input) {
            controllableManager.applyControllable(input);
            controllableManager.applySuspendable(input);
        }
    }


    /**
     * The ScopeResolutionStage will translate a list of scope entries (specified in the PlanScope)
     * into a list of "seed" entity oid's representing the focus on the topology to be analyzed.
     * These "seed entities" will be written into to the TopologyInfo for inclusion in the
     * broadcast.
     *
     * Each scope entry will refer either to a specific entity, or to a group. Scope entry id's for
     * specific entities will be directly to the seed entity list. Scope entry id's for "Group"
     * types will be resolved to a list of group member entities, and each of those will be added
     * to the "seed entities" list.
     */
    public static class ScopeResolutionStage extends PassthroughStage<TopologyGraph> {
        private static final Logger logger = LogManager.getLogger();

        private static final String GROUP_TYPE = "Group";
        private static final String CLUSTER_TYPE = "Cluster";
        private static final Set<String> GROUP_TYPES = Sets.newHashSet(GROUP_TYPE,CLUSTER_TYPE);
        private final PlanScope planScope;

        private final GroupServiceBlockingStub groupServiceClient;

        public ScopeResolutionStage(@Nonnull GroupServiceBlockingStub groupServiceClient,
                                    @Nullable final PlanScope planScope) {
            this.groupServiceClient = groupServiceClient;
            this.planScope = planScope;
        }

        @Override
        public void passthrough(final TopologyGraph input) throws PipelineStageException {
            // if no scope to apply, this function does nothing.
            if (planScope.getScopeEntriesCount() == 0) {
                return;
            }
            // translate the set of groups and entities in the plan scope into a list of "seed"
            // entities for the market to focus on. The list of "seeds" should include all seeds
            // already in the list, all entities individually specified in the scope, as well as all
            // entities belonging to groups that are in scope.

            // initialize the set with any existing seed oids
            Set<Long> seedEntities = new HashSet(getContext().getTopologyInfo().getScopeSeedOidsList());
            Set<Long> groupsToResolve = new HashSet();
            for (PlanScopeEntry scopeEntry : planScope.getScopeEntriesList()) {
                // if it's an entity, add it right to the seed entity list. Otherwise queue it for
                // group resolution.
                if (GROUP_TYPES.contains(scopeEntry.getClassName())) {
                    groupsToResolve.add(scopeEntry.getScopeObjectOid());
                } else {
                    // this is an entity -- add it right to the seedEntities
                    seedEntities.add(scopeEntry.getScopeObjectOid());
                }
            }

            // Now resolve the groups by adding all the group members to the seedEntities
            if (groupsToResolve.size() > 0) {
                // fetch the group definitions from the group service
                GetGroupsRequest request = GetGroupsRequest.newBuilder()
                        .addAllId(groupsToResolve)
                        .setResolveClusterSearchFilters(true)
                        .build();
                groupServiceClient.getGroups(request)
                    .forEachRemaining(
                        group -> {
                            try {
                                // add each group's members to the set of additional seed entities
                                Set<Long> groupMemberOids = getContext().getGroupResolver().resolve(group, input);
                                seedEntities.addAll(groupMemberOids);
                            } catch (GroupResolutionException gre) {
                                // log a warning
                                logger.warn("Couldn't resolve members for group {} while defining scope", group);
                            }
                        }
                    );
            }

            // now add the new seed entities to the list
            getContext().editTopologyInfo(topologyInfoBuilder -> {
                topologyInfoBuilder.clearScopeSeedOids();
                topologyInfoBuilder.addAllScopeSeedOids(seedEntities);
            });
        }
    }

    /**
     * This stage creates a {@link TopologyGraph} out of the topology entities.
     */
    public static class GraphCreationStage extends Stage<Map<Long, TopologyEntity.Builder>, TopologyGraph> {

        @Nonnull
        @Override
        public TopologyGraph execute(@Nonnull final Map<Long, TopologyEntity.Builder> input) {
            return TopologyGraph.newGraph(input);
        }
    }

    /**
     * Stage to apply changes to commodities values like used,peak etc.
     */
    public static class CommoditiesEditStage extends PassthroughStage<TopologyGraph> {

        private final List<ScenarioChange> changes;

        private final CommoditiesEditor commoditiesEditor;

        private final PlanScope scope;


        public CommoditiesEditStage(@Nonnull CommoditiesEditor commoditiesEditor,
                        @Nonnull List<ScenarioChange> changes, @Nonnull PlanScope scope) {
            this.changes = Objects.requireNonNull(changes);
            this.commoditiesEditor = Objects.requireNonNull(commoditiesEditor);
            this.scope = Objects.requireNonNull(scope);
        }

        @Override
        public void passthrough(@Nonnull TopologyGraph graph) throws PipelineStageException {
            commoditiesEditor.applyCommodityEdits(graph, changes, getContext().getTopologyInfo(), scope);
        }
    }

    /**
     * Stage to apply some constraint-specific changes like disabling of ignored commodities.
     */
    public static class IgnoreConstraintsStage extends PassthroughStage<TopologyGraph> {

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

        @Override
        public void passthrough(TopologyGraph input) throws PipelineStageException {
            boolean isPressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(getContext().getTopologyInfo());
            constraintsEditor.editConstraints(input, changes, isPressurePlan);
        }
    }

    /**
     * This stage applies policies to a {@link TopologyGraph}. This makes changes
     * to the commodities of entities in the {@link TopologyGraph} to reflect the
     * applied policies.
     */
    public static class PolicyStage extends PassthroughStage<TopologyGraph> {

        private final PolicyManager policyManager;
        private final List<ScenarioChange> changes;

        public PolicyStage(@Nonnull final PolicyManager policyManager) {
            this(policyManager, Collections.emptyList());
        }

        public PolicyStage(@Nonnull final PolicyManager policyManager, List<ScenarioChange> changes) {
            this.policyManager = policyManager;
            this.changes = Objects.requireNonNull(changes);
        }

        @Override
        public void passthrough(@Nonnull final TopologyGraph input) {
            policyManager.applyPolicies(input, getContext().getGroupResolver(), changes);
        }
    }

    /**
     * This stages resolves per-entity settings in the {@link TopologyGraph}.
     * It's responsible for determining which settings apply to which entities, performing
     * conflict resolution (when multiple setting policies apply to a single entity), and
     * applying setting overrides from plan scenarios.
     */
    public static class SettingsResolutionStage extends Stage<TopologyGraph, GraphWithSettings> {
        private final Logger logger = LogManager.getLogger();

        private final SettingOverrides settingOverrides;

        private final EntitySettingsResolver entitySettingsResolver;

        private SettingsResolutionStage(@Nonnull final EntitySettingsResolver entitySettingsResolver,
                                        @Nonnull final List<ScenarioChange> scenarioChanges) {
            this.entitySettingsResolver = entitySettingsResolver;
            this.settingOverrides = new SettingOverrides(scenarioChanges);
        }

        public static SettingsResolutionStage live(@Nonnull final EntitySettingsResolver entitySettingsResolver) {
            return new SettingsResolutionStage(entitySettingsResolver, Collections.emptyList());
        }

        public static SettingsResolutionStage plan(
                @Nonnull final EntitySettingsResolver entitySettingsResolver,
                @Nonnull final List<ScenarioChange> scenarioChanges) {
            return new SettingsResolutionStage(entitySettingsResolver, scenarioChanges);
        }

        @Nonnull
        @Override
        public GraphWithSettings execute(@Nonnull final TopologyGraph topologyGraph) {
            try {
                return entitySettingsResolver.resolveSettings(getContext().getGroupResolver(),
                        topologyGraph, settingOverrides, getContext().getTopologyInfo());
            } catch (RuntimeException e) {
                logger.error("Error resolving settings for graph", e);
                return new GraphWithSettings(topologyGraph,
                        Collections.emptyMap(), Collections.emptyMap());
            }
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

        @Override
        public void passthrough(final GraphWithSettings input) throws PipelineStageException {
            // This method does a sync call to Group Component.
            // If GC has trouble, the topology broadcast would be delayed.
            entitySettingsResolver.sendEntitySettings(getContext().getTopologyInfo(),
                    input.getEntitySettings());
        }
    }

    /**
     * This stage applies settings resolved in {@link SettingsResolutionStage} to the
     * {@link TopologyGraph} the settings got resolved on. For example, if "suspend" is disabled
     * for entity 10, this stage is responsible for making sure that the relevant property
     * in {@link TopologyEntityDTO} reflects that.
     */
    public static class SettingsApplicationStage extends PassthroughStage<GraphWithSettings> {
        private final EntitySettingsApplicator settingsApplicator;

        public SettingsApplicationStage(@Nonnull final EntitySettingsApplicator settingsApplicator) {
            this.settingsApplicator = settingsApplicator;
        }

        @Override
        public void passthrough(final GraphWithSettings input) throws PipelineStageException {
            settingsApplicator.applySettings(getContext().getTopologyInfo(), input);
        }
    }

    /**
     * This stage applies post-stitching operations to apply additional stitching operations to the topology
     * that operate in a context with settings available. For additional details {@see PostStitchingOperation}.
     *
     * Post-stitching should be applied in the same pipelines as the main stitching phase.
     * {@see StitchingStage}.
     */
    public static class PostStitchingStage extends PassthroughStage<GraphWithSettings> {

        private final StitchingManager stitchingManager;

        public PostStitchingStage(@Nonnull final StitchingManager stitchingManager) {
            this.stitchingManager = stitchingManager;
        }

        @Override
        public void passthrough(final GraphWithSettings input) throws PipelineStageException {
            // Set up the post-stitching journal.
            final IStitchingJournal<StitchingEntity> mainJournal = getContext()
                .getStitchingJournalContainer()
                .getMainStitchingJournal()
                .orElse(StitchingJournalFactory.emptyStitchingJournalFactory().stitchingJournal(null));
            final IStitchingJournal<TopologyEntity> postStitchingJournal = mainJournal.childJournal(
                new TopologyEntitySemanticDiffer(mainJournal.getJournalOptions().getVerbosity()));
            getContext().getStitchingJournalContainer().setPostStitchingJournal(postStitchingJournal);

            stitchingManager.postStitch(input, postStitchingJournal);

            if (postStitchingJournal.shouldDumpTopologyAfterPostStitching()) {
                postStitchingJournal.dumpTopology(input.getTopologyGraph().entities());
            }
        }
    }

    /**
     * This stage validates all entities within a graph, and sets any illegal commodity values to
     * appropriate substitutes.
     *
     * This stage should occur after post-stitching; if it happens before, some commodity values
     * may be erroneously substituted and some necessary post-stitching operations may not occur.
     */
    public static class EntityValidationStage extends PassthroughStage<GraphWithSettings> {
        private final EntityValidator entityValidator;

        public EntityValidationStage(@Nonnull final EntityValidator entityValidator) {
            this.entityValidator = entityValidator;
        }

        @Override
        public void passthrough(final GraphWithSettings input) throws PipelineStageException {
            try {
                entityValidator.validateTopologyEntities(input.getTopologyGraph().entities());
            } catch (EntitiesValidationException e) {
                throw new PipelineStageException(e);
            }

        }

        @Override
        protected boolean required() {
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

        @Override
        public void passthrough(final GraphWithSettings input) throws PipelineStageException {
            supplyChainValidator.validateTopologyEntities(input.getTopologyGraph().entities());
        }

        @Override
        protected boolean required() {
            return true;
        }
    }

    /**
     * Placeholder stage to extract the {@link TopologyGraph} for the {@link BroadcastStage}
     * in the live and plan (but not plan-over-plan) topology.
     *
     * Also records {@link TopologyInfo} to the {@link StitchingJournal}.
     *
     * We shouldn't need this once plan-over-plan supports policies and settings.
     */
    public static class ExtractTopologyGraphStage extends Stage<GraphWithSettings, TopologyGraph> {
        @Nonnull
        @Override
        public TopologyGraph execute(@Nonnull final GraphWithSettings graphWithSettings)
                throws PipelineStageException, InterruptedException {
            return graphWithSettings.getTopologyGraph();
        }
    }

    /**
     * This stage broadcasts the topology represented by a {@link TopologyGraph} out of the
     * topology processor.
     */
    public static class BroadcastStage extends Stage<TopologyGraph, TopologyBroadcastInfo> {

        private final Logger logger = LogManager.getLogger();

        private final List<TopoBroadcastManager> broadcastManagers;

        public BroadcastStage(@Nonnull final List<TopoBroadcastManager> broadcastManagers) {
            this.broadcastManagers = Objects.requireNonNull(broadcastManagers);
            Preconditions.checkArgument(broadcastManagers.size() > 0);
        }

        @Nonnull
        @Override
        public TopologyBroadcastInfo execute(@Nonnull final TopologyGraph input)
                throws PipelineStageException, InterruptedException {

            // Record TopologyInfo and Metrics to the journal if there is one.
            getContext().getStitchingJournalContainer().getPostStitchingJournal().ifPresent(journal -> {
                journal.recordTopologyInfoAndMetrics(getContext().getTopologyInfo(), journal.getMetrics());
                journal.flushRecorders();
            });

            final Iterator<TopologyEntityDTO> entities = input.entities()
                    .map(topologyEntity -> topologyEntity.getTopologyEntityDtoBuilder().build())
                    .iterator();
            try {
                switch (getContext().getTopologyInfo().getTopologyType()) {
                    case REALTIME:
                        return broadcastTopology(broadcastManagers.stream()
                            .<Function<TopologyInfo, TopologyBroadcast>>map(manager -> manager::broadcastLiveTopology)
                            .collect(Collectors.toList()), entities);
                    case PLAN:
                        return broadcastTopology(broadcastManagers.stream()
                            .<Function<TopologyInfo, TopologyBroadcast>>map(manager -> manager::broadcastUserPlanTopology)
                            .collect(Collectors.toList()), entities);
                    default:
                        throw new IllegalStateException();
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
                    broadcast.append(entity);
                }
            }

            long resultSentCount = 0;
            for (TopologyBroadcast broadcast : broadcasts) {
                final long sentCount = broadcast.finish();
                resultSentCount = sentCount;

                logger.info("Successfully sent {} entities within topology {} for context {} " +
                        "and broadcast of type {}", sentCount, topologyInfo.getTopologyId(),
                    topologyInfo.getTopologyContextId(), broadcast.getClass().getSimpleName());
            }

            return new TopologyBroadcastInfo(resultSentCount, topologyInfo.getTopologyId(),
                topologyInfo.getTopologyContextId());
        }
    }
}
