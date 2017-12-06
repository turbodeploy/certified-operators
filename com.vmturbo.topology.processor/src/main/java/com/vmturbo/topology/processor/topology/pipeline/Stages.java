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

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.group.settings.SettingOverrides;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.topology.ConstraintsEditor;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;

/**
 * A wrapper class for the various {@link Stage} and {@link PassthroughStage} implementations.
 * Since the stages are pretty small, it makes some sense to keep them in one place for now.
 */
public class Stages {

    /**
     * This stage uploads discovered groups (and clusters) to the group component.
     * We only do this for the live topology.
     */
    public static class UploadGroupsStage extends PassthroughStage<EntityStore> {

        private final DiscoveredGroupUploader discoveredGroupUploader;

        public UploadGroupsStage(@Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
            this.discoveredGroupUploader = discoveredGroupUploader;
        }

        @Nonnull
        @Override
        public void passthrough(final EntityStore input) {
            discoveredGroupUploader.processQueuedGroups();
        }
    }

    /**
     * This stage uploads discovered templates and deployment profiles to the plan orchestrator.
     * We only do this for the live topology.
     */
    public static class UploadTemplatesStage extends PassthroughStage<EntityStore> {

        private final DiscoveredTemplateDeploymentProfileNotifier notifier;

        public UploadTemplatesStage(@Nonnull final DiscoveredTemplateDeploymentProfileNotifier notifier) {
            this.notifier = notifier;
        }

        @Nonnull
        @Override
        public void passthrough(final EntityStore input) throws PipelineStageException {
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
    public static class StitchingStage extends Stage<EntityStore, Map<Long, Builder>> {

        private final StitchingManager stitchingManager;

        public StitchingStage(@Nonnull final StitchingManager stitchingManager) {
            this.stitchingManager = stitchingManager;
        }

        @Nonnull
        @Override
        public Map<Long, TopologyEntityDTO.Builder> execute(@Nonnull final EntityStore entityStore) {
            return stitchingManager.stitch(entityStore).constructTopology();
        }
    }

    /**
     * This stage acquires an old topology from the repository.
     *
     * We only do this in plans, because the live topology pipeline doesn't depend
     * on old topologies.
     */
    public static class TopologyAcquisitionStage extends Stage<Long, Map<Long, Builder>> {

        private final RepositoryClient repository;

        public TopologyAcquisitionStage(@Nonnull final RepositoryClient repositoryClient) {
            this.repository = Objects.requireNonNull(repositoryClient);
        }

        @Nonnull
        @Override
        public Map<Long, Builder> execute(@Nonnull final Long topologyId) {
            // we need to gather the entire topology in order to perform editing below
            Iterable<RepositoryDTO.RetrieveTopologyResponse> dtos =
                    () -> repository.retrieveTopology(topologyId);
            return StreamSupport.stream(dtos.spliterator(), false)
                    .map(RepositoryDTO.RetrieveTopologyResponse::getEntitiesList)
                    .flatMap(List::stream)
                    .map(TopologyEntityDTO::toBuilder)
                    .collect(Collectors.toMap(TopologyEntityDTOOrBuilder::getOid, Function.identity()));
        }
    }

    /**
     * This stage applies a set of {@link ScenarioChange} objects to a topology.
     *
     * We only do this in plans because only plans have scenario changes on top of a topology.
     */
    public static class TopologyEditStage extends PassthroughStage<Map<Long, Builder>> {

        private final TopologyEditor topologyEditor;
        private final List<ScenarioChange> changes;

        public TopologyEditStage(@Nonnull final TopologyEditor topologyEditor,
                                 @Nonnull final List<ScenarioChange> scenarioChanges) {
            this.topologyEditor = Objects.requireNonNull(topologyEditor);
            this.changes = Objects.requireNonNull(scenarioChanges);
        }

        @Override
        public void passthrough(@Nonnull final Map<Long, TopologyEntityDTO.Builder> input) {
            topologyEditor.editTopology(input, changes);
        }

        @Override
        public boolean required() {
            return true;
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
    public static class GraphCreationStage extends Stage<Map<Long, Builder>, TopologyGraph> {

        @Nonnull
        @Override
        public TopologyGraph execute(@Nonnull final Map<Long, TopologyEntityDTO.Builder> input) {
            return new TopologyGraph(input);
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
            constraintsEditor.editConstraints(input, changes);
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
                        topologyGraph, settingOverrides);
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
     * Placeholder stage to extract the {@link TopologyGraph} for the {@link BroadcastStage}
     * in the live and plan (but not plan-over-plan) topology.
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

        private final TopoBroadcastManager broadcastManager;

        public BroadcastStage(@Nonnull final TopoBroadcastManager broadcastManager) {
            this.broadcastManager = broadcastManager;
        }

        @Nonnull
        @Override
        public TopologyBroadcastInfo execute(@Nonnull final TopologyGraph input)
                throws PipelineStageException, InterruptedException {
            final Iterator<TopologyEntityDTO> entities = input.vertices()
                    .map(vertex -> vertex.getTopologyEntityDtoBuilder().build())
                    .iterator();
            try {
                switch (getContext().getTopologyInfo().getTopologyType()) {
                    case REALTIME:
                        return broadcastTopology(broadcastManager::broadcastLiveTopology, entities);
                    case PLAN:
                        return broadcastTopology(broadcastManager::broadcastUserPlanTopology, entities);
                    default:
                        throw new IllegalStateException();
                }
            } catch (CommunicationException e) {
                throw new PipelineStageException(e);
            }
        }

        private TopologyBroadcastInfo broadcastTopology(
                @Nonnull Function<TopologyInfo, TopologyBroadcast> broadcastFunction,
                final Iterator<TopologyEntityDTO> topology)
                throws InterruptedException, CommunicationException {
            final TopologyBroadcast broadcast = broadcastFunction.apply(getContext().getTopologyInfo());
            for (Iterator<TopologyEntityDTO> it = topology; it.hasNext(); ) {
                final TopologyEntityDTO entity = it.next();
                broadcast.append(entity);
            }
            final long sentCount = broadcast.finish();
            logger.info("Successfully sent {} entities within topology {} for context {}.", sentCount,
                    broadcast.getTopologyId(), broadcast.getTopologyContextId());
            return new TopologyBroadcastInfo(broadcast, sentCount);
        }
    }
}
