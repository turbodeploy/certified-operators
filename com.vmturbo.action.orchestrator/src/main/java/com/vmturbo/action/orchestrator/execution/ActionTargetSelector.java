package com.vmturbo.action.orchestrator.execution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilities;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Select the target/probe to execute an action against. It is also responsible for breaking ties when
 * multiple targets are eligible for action execution.
 */
public class ActionTargetSelector {

    private static final ActionTargetInfo UNSUPPORTED = ImmutableActionTargetInfo.builder()
        .supportingLevel(SupportLevel.UNSUPPORTED)
        .build();

    private static final ActionTargetInfo SHOW_ONLY = ImmutableActionTargetInfo.builder()
        .supportingLevel(SupportLevel.SHOW_ONLY)
        .build();

    private static final Logger logger = LogManager.getLogger();

    private final TargetInfoResolver targetInfoResolver;

    /**
     * Chooses an entity to execute an action against
     */
    private final ActionExecutionEntitySelector entitySelector;

    /**
     * A client for making remote calls to the Repository service to retrieve entity data
     */
    private final RepositoryServiceBlockingStub repositoryService;

    /**
     * The context ID for the realtime market. Used when making remote calls to the repository service.
     */
    private final long realtimeTopologyContextId;

    /**
     * Create an ActionTargetSelector.
     * @param probeCapabilityCache To get the target-specific action capabilities.
     * @param entitySelector to select a service entity to execute an action against
     * @param repositoryProcessorChannel Channel to access repository.
     * @param realtimeTopologyContextId the context ID of the realtime market
     */
    public ActionTargetSelector(@Nonnull final ProbeCapabilityCache probeCapabilityCache,
                                @Nonnull final ActionExecutionEntitySelector entitySelector,
                                @Nonnull final Channel repositoryProcessorChannel,
                                final long realtimeTopologyContextId) {
        this(new TargetInfoResolver(probeCapabilityCache), entitySelector,
            repositoryProcessorChannel, realtimeTopologyContextId);
    }

    @VisibleForTesting
    ActionTargetSelector(@Nonnull final TargetInfoResolver targetInfoResolver,
                         @Nonnull final ActionExecutionEntitySelector entitySelector,
                         @Nonnull final Channel repositoryProcessorChannel,
                         final long realtimeTopologyContextId) {
        this.targetInfoResolver = Objects.requireNonNull(targetInfoResolver);
        this.entitySelector = Objects.requireNonNull(entitySelector);
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(repositoryProcessorChannel);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Describes the target resolution result for a particular {@link ActionDTO.Action}.
     */
    @Value.Immutable
    public interface ActionTargetInfo {
        /**
         * The support level for the action in the Turbonomic system.
         */
        SupportLevel supportingLevel();

        /**
         * The OID of the target that should execute this action. Should always be set if
         * {@link ActionTargetInfo#supportingLevel()} is {@link SupportLevel#SUPPORTED}. May not be
         * set otherwise.
         */
        Optional<Long> targetId();

        /**
         * The pre-requisites for an action.
         *
         * @return a set of {@link Prerequisite}s
         */
        Set<Prerequisite> prerequisites();
    }

    /**
     * Return the {@link ActionTargetInfo} for an action. Makes an RPC call internally.
     * If you need to do this for more than
     * one action, use {@link ActionTargetSelector#getTargetsForActions(Stream, EntitiesAndSettingsSnapshot)}.
     *
     * @param action The {@link ActionDTO.Action} generated by the market.
     * @param entitySettingsCache an entity snapshot factory
     * @return An {@link ActionTargetInfo} describing the support + target id for the action.
     */
    @Nonnull
    public ActionTargetInfo getTargetForAction(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache) {
        final Map<Long, ActionTargetInfo> actionToTargetInfo =
            getTargetsForActions(Stream.of(action), entitySettingsCache.emptySnapshot());
        ActionTargetInfo retInfo = actionToTargetInfo.get(action.getId());
        return retInfo != null ? retInfo : UNSUPPORTED;
    }

    /**
     * Returns an {@link ActionTargetInfo} for each provided action.
     * This is the "bulk" version of
     * {@link ActionTargetSelector#getTargetForAction(ActionDTO.Action, EntitiesAndSettingsSnapshotFactory)}.
     *
     * <p>This method retrieves the target resolution data (support level and targetId) for each
     * action in the actions Stream. Entity data for entities associated with each action is required,
     * and will be retrieved from the entityMap (if provided) or else will be retrieved using a remote
     * call to Repository which will retrieve entity data from the realtime market.</p>
     *
     * <p>TODO: provide a flag to cause an explanation/failure to be reported for unsupported actions
     * where applicable.</p>
     *
     * @param actions Stream of action recommendations.
     * @param snapshot The snapshot of entities.
     * @return Map of (action id) to ({@link ActionTargetInfo}) for the action, for each
     *         action in the input.
     */
    @Nonnull
    public Map<Long, ActionTargetInfo> getTargetsForActions(
            @Nonnull final Stream<ActionDTO.Action> actions,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        final Map<Long, ActionResolutionData> resolutionStateByAction = actions
            .collect(Collectors.toMap(ActionDTO.Action::getId, ActionResolutionData::new));
        for (final ActionResolutionData resolutionState : resolutionStateByAction.values()) {
            try {
                // Select the entity to execute the action against, and put the result in the map
                final ActionEntity executantEntity = getExecutantEntity(resolutionState.recommendation);
                resolutionState.setExecutantEntity(executantEntity);
            } catch (EntitiesResolutionException | UnsupportedActionException e) {
                // If there was a problem determining the execution target for this action, log the
                // exception and mark the action as unsupported.
                // TODO (roman, March 18 2019): Should we exclude these actions from the output
                // entirely? For now we leave it up to the caller to filter out unsupported actions
                // if they don't care about them.
                logger.warn("An entity could not be selected for executing this action. actionId: "
                    + resolutionState.recommendation.getId());
                logger.debug(e.getMessage(), e);
                resolutionState.setTargetInfo(UNSUPPORTED);
            }
        }

        final Map<Long, ActionPartialEntity> actionPartialEntityMap;
        if (snapshot.getEntityMap().isEmpty()) {
            // We need to fetch the partial action entities from repository.
            Set<Long> involvedEntities =
                ActionDTOUtil.getInvolvedEntityIds(resolutionStateByAction.values()
                    .stream().map(actionResolutionData -> actionResolutionData.recommendation)
                    .collect(Collectors.toList()));
            actionPartialEntityMap = getRealtimeActionEntities(involvedEntities);
        } else {
            actionPartialEntityMap = snapshot.getEntityMap();
        }

        try {
            for (Entry<Long, ActionResolutionData> actionIdAndResolutionState : resolutionStateByAction.entrySet()) {
                final ActionResolutionData resolutionState = actionIdAndResolutionState.getValue();
                final ActionDTO.Action recommendation = resolutionState.recommendation;
                final ActionEntity executantEntity = resolutionState.getExecutantEntity();
                if (executantEntity != null) {
                    ActionPartialEntity actionPartialEntity =
                        actionPartialEntityMap.get(executantEntity.getId());
                    if (actionPartialEntity != null) {
                        // Get an id (either a targetId or a probeId, depending on the provided function)
                        resolutionState.resolvedTargetInfo =
                            targetInfoResolver.getTargetInfoForAction(recommendation,
                                executantEntity, actionPartialEntity, snapshot);
                    } else {
                        logger.warn("Selected entity {} involved in action {} no longer present in system.",
                            resolutionState, recommendation.getId());
                        resolutionState.setTargetInfo(UNSUPPORTED);
                    }
                }
            }

        } catch (StatusRuntimeException e) {
            // If we can't get the information from the topology processor, default the support
            // level for all actions that have targets should be SHOW_ONLY.
            logger.error("Failed to get entity infos from the topology processor. Error: {}",
                e.getMessage());
        }

        return resolutionStateByAction.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().toTargetInfo()));
    }

    /**
     * Get the entity ID to use for the {@link ExecutableStep} of an {@link Action}.
     * While this defaults to the primary entity of the action--the entity being acted upon--there
     * exist special cases where a different, related entity needs to be used in order to execute
     * an action.
     *
     * @param action TopologyProcessor Action
     * @return {@link ActionEntity} for the selected entity
     * @throws EntitiesResolutionException if entities related to the target failed to
     *         resolve in TopologyProcessor
     */
    private ActionEntity getExecutantEntity(@Nonnull ActionDTO.Action action)
            throws EntitiesResolutionException, UnsupportedActionException {
        // Check for special cases using the entitySelector
        return entitySelector.getEntity(action)
                .orElseThrow(() -> new EntitiesResolutionException(
                        "No entity could be found for this action: " + action.toString()));

    }

    /**
     * Return the {@link ActionPartialEntity} for the given entity ids from the realtime topology.
     *
     * @param entityIds The id of the entities.
     * @return a {@link Map} with an {@link ActionPartialEntity} per entity id.
     */
    private Map<Long, ActionPartialEntity> getRealtimeActionEntities(Set<Long> entityIds) {
        RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest.newBuilder()
            .setTopologyType(TopologyType.SOURCE)
            .addAllEntityOids(entityIds)
            .setReturnType(PartialEntity.Type.ACTION)
            .setTopologyContextId(realtimeTopologyContextId)
            .build();
        return
            RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(getEntitiesrequest))
                .map(PartialEntity::getAction)
                .collect(Collectors.toMap(ActionPartialEntity::getOid, Function.identity()));
    }

    @VisibleForTesting
    static class TargetInfoResolver {

        /**
         * If multiple probes support the same action, this priority is used to determine which target
         * will execute the action. Position of category in list determines priority. List contains
         * highest priority categories which should be executed firstly at the begin of it and lowest
         * priority categories at the end.
         **/
        public static final List<ProbeCategory> PROBE_CATEGORY_PRIORITIES =
            ImmutableList.of(
                ProbeCategory.CLOUD_NATIVE,
                ProbeCategory.CLOUD_MANAGEMENT,
                ProbeCategory.STORAGE,
                ProbeCategory.FABRIC,
                ProbeCategory.HYPERVISOR,
                ProbeCategory.APPLICATION_SERVER,
                ProbeCategory.DATABASE_SERVER,
                ProbeCategory.CUSTOM,
                ProbeCategory.GUEST_OS_PROCESSES);

        public static final int UNSPECIFIED_PRIORITY = PROBE_CATEGORY_PRIORITIES.size() + 1;
        public static final int NO_CATEGORY_PRIORITY = UNSPECIFIED_PRIORITY + 1;

        private final ProbeCapabilityCache probeCapabilityCache;

        TargetInfoResolver(@Nonnull final ProbeCapabilityCache probeCapabilityCache) {
            this.probeCapabilityCache = Objects.requireNonNull(probeCapabilityCache);
        }

        /**
         * Get the {@link ActionTargetInfo} of this action.
         *
         * @param action TopologyProcessor Action
         * @param executantEntity an EntityInfo that provides target/probe data which will be used to
         *                        execute this action.
         * @param actionPartialEntity the target actionPartialEntity of this action
         * @param snapshot the snapshot of entities
         * @return {@link ActionTargetInfo} of this action
         */
        @VisibleForTesting
        ActionTargetInfo getTargetInfoForAction(@Nonnull final ActionDTO.Action action,
                                                @Nonnull final ActionDTO.ActionEntity executantEntity,
                                                @Nonnull final ActionPartialEntity actionPartialEntity,
                                                @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
            final CachedCapabilities cachedCapabilities = probeCapabilityCache.getCachedCapabilities();
            SupportLevel bestSupportLevel = SupportLevel.UNSUPPORTED;
            Long bestTargetForSupportLevel = null;
            final Set<Prerequisite> prerequisites = new HashSet<>();

            for (Long targetId : actionPartialEntity.getDiscoveringTargetIdsList()) {
                Optional<Long> probeIdOptional = cachedCapabilities.getProbeFromTarget(targetId);
                if (probeIdOptional.isPresent()) {
                    // Calculate support level.
                    final SupportLevel targetSupportLevel =
                        cachedCapabilities.getSupportLevel(action, executantEntity, probeIdOptional.get());
                    // Check for unknown explicitly because it's a lower index than all others.
                    if (targetSupportLevel != SupportLevel.UNKNOWN &&
                        targetSupportLevel.getNumber() < bestSupportLevel.getNumber()) {
                        // Update the best support level encountered so far.
                        bestSupportLevel = targetSupportLevel;
                        // The first target encountered at the "better" support level is the best.
                        bestTargetForSupportLevel = targetId;
                    } else if (targetSupportLevel == bestSupportLevel) {
                        final int curBestPriority = getTargetPriority(bestTargetForSupportLevel,
                            cachedCapabilities);
                        final int thisTargetPriority = getTargetPriority(targetId,
                            cachedCapabilities);
                        // A lower priority value is considered higher priority (because it's order
                        // in a descending list). If this target's priority is lower than the best
                        // so far, this should be the best one.
                        if (curBestPriority > thisTargetPriority) {
                            bestTargetForSupportLevel = targetId;
                        }
                    }

                    // Calculate pre-requisite.
                    cachedCapabilities.getProbeCategory(probeIdOptional.get())
                        .ifPresent(probeCategory -> prerequisites.addAll(
                            PrerequisiteCalculator.calculatePrerequisites(
                                action, actionPartialEntity, snapshot, probeCategory)));
                }
            }

            // This should never happen, because we set support level to UNSUPPORTED initially,
            // and if we change it to anything else we also set the target ID. But we want to
            // make extra-sure to never return a SUPPORTED action with no target that can execute it.
            if (bestSupportLevel == SupportLevel.SUPPORTED && bestTargetForSupportLevel == null) {
                logger.error("Supported/show-only action (id: {}) has no target ID. " +
                    "Changing support level to show-only", action.getId());
                bestSupportLevel = SupportLevel.SHOW_ONLY;
            }

            final ImmutableActionTargetInfo.Builder retBuilder = ImmutableActionTargetInfo.builder()
                .supportingLevel(bestSupportLevel);
            if (bestTargetForSupportLevel != null) {
                retBuilder.targetId(bestTargetForSupportLevel);
            }
            retBuilder.prerequisites(prerequisites);
            return retBuilder.build();
        }

        private int getTargetPriority(@Nullable final Long targetId,
                                      @Nonnull final CachedCapabilities cachedCapabilities) {
            if (targetId == null) {
                return UNSPECIFIED_PRIORITY;
            }

            // The targets should be present in the map, because we get the target values
            // from the map in the calling method. However, we check for nulls just in case to
            // avoid NPEs if the calling patterns change.
            Optional<Long> probeId = cachedCapabilities.getProbeFromTarget(targetId);
            if (!probeId.isPresent()) {
                logger.error("Unexpected null - no probe ID found for target ID {}", targetId);
                return UNSPECIFIED_PRIORITY;
            }

            return cachedCapabilities.getProbeCategory(probeId.get())
                .map(category -> {
                    int index = PROBE_CATEGORY_PRIORITIES.indexOf(category);
                    if (index >= 0) {
                        return index;
                    } else {
                        // All probe categories not in the explicit list of priorities go
                        // to the bottom of the priority pile.
                        return NO_CATEGORY_PRIORITY;
                    }
                })
                // If we don't even have a probe category,
                .orElse(UNSPECIFIED_PRIORITY);
        }

        private Optional<Long> highestPriorityTarget(
                @Nonnull final Set<Long> targets,
                @Nonnull final Map<Long, Long> targetIdToProbeIdMap,
                @Nonnull final CachedCapabilities cachedCapabilities) {
            if (targets.size() == 1) {
                return Optional.of(targets.iterator().next());
            } else if (targets.isEmpty()) {
                return Optional.empty();
            } else {
                final Map<Long, Integer> targetIdsToPriorities = new HashMap<>();
                for (Long target : targets) {
                    cachedCapabilities.getProbeCategory(targetIdToProbeIdMap.get(target))
                        .ifPresent(category -> {
                            int index = PROBE_CATEGORY_PRIORITIES.indexOf(category);
                            if (index >= 0) {
                                targetIdsToPriorities.put(target, index);
                            } else {
                                // All probe categories not in the explicit list of priorities go
                                // to the bottom of the priority pile.
                                targetIdsToPriorities.put(target, PROBE_CATEGORY_PRIORITIES.size() + 1);
                            }
                        });
                }
                return targetIdsToPriorities.entrySet().stream()
                    .min(Entry.comparingByValue())
                    .map(Entry::getKey);
            }
        }
    }

    /**
     * Internal state object to manage action target resolution.
     * Used to avoid cross-referencing between multiple maps.
     */
    private static class ActionResolutionData {
        private final ActionDTO.Action recommendation;
        private ActionEntity executantEntity = null;
        private ActionTargetInfo resolvedTargetInfo = null;

        private ActionResolutionData(final ActionDTO.Action recommendation) {
            this.recommendation = recommendation;
        }

        @Nonnull
        ActionTargetInfo toTargetInfo() {
            if (resolvedTargetInfo != null) {
                return resolvedTargetInfo;
            } else {
                return SHOW_ONLY;
            }
        }

        public void setTargetInfo(@Nonnull final ActionTargetInfo actionTargetInfo) {
            this.resolvedTargetInfo = actionTargetInfo;
        }

        public void setExecutantEntity(@Nonnull final ActionEntity executantEntity) {
            this.executantEntity = executantEntity;
        }

        @Nullable
        public ActionEntity getExecutantEntity() {
            return executantEntity;
        }
    }
}
