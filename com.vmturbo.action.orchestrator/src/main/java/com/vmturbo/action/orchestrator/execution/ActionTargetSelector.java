package com.vmturbo.action.orchestrator.execution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
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
     * A client for making remote calls to the Topology Processor service to retrieve entity data
     */
    private final EntityServiceGrpc.EntityServiceBlockingStub entityServiceBlockingStub;

    /**
     * Create an ActionTargetSelector
     * @param probeCapabilityCache To get the target-specific action capabilities.
     * @param entitySelector to select a service entity to execute an action against
     * @param topologyProcessorChannel Channel to access the topology processor.
     */
    public ActionTargetSelector(@Nonnull final ProbeCapabilityCache probeCapabilityCache,
                                @Nonnull final ActionExecutionEntitySelector entitySelector,
                                @Nonnull final Channel topologyProcessorChannel) {
        this(new TargetInfoResolver(probeCapabilityCache), entitySelector, topologyProcessorChannel);
    }

    @VisibleForTesting
    ActionTargetSelector(@Nonnull final TargetInfoResolver targetInfoResolver,
                         @Nonnull final ActionExecutionEntitySelector entitySelector,
                         @Nonnull final Channel topologyProcessorChannel) {
        this.targetInfoResolver = Objects.requireNonNull(targetInfoResolver);
        this.entitySelector = Objects.requireNonNull(entitySelector);
        this.entityServiceBlockingStub = EntityServiceGrpc.newBlockingStub(
            Objects.requireNonNull(topologyProcessorChannel));
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
    }

    /**
     * Return the {@link ActionTargetInfo} for an action. Makes an RPC call internally.
     * If you need to do this for more than
     * one action, use {@link ActionTargetSelector#getTargetsForActions(Stream)}.
     *
     * @param action The {@link ActionDTO.Action} generated by the market.
     * @return An {@link ActionTargetInfo} describing the support + target id for the action.
     */
    @Nonnull
    public ActionTargetInfo getTargetForAction(final ActionDTO.Action action) {
        final Map<Long, ActionTargetInfo> actionToTargetInfo =
            getTargetsForActions(Stream.of(action));
        ActionTargetInfo retInfo = actionToTargetInfo.get(action.getId());
        return retInfo != null ? retInfo : UNSUPPORTED;
    }

    /**
     * Returns an {@link ActionTargetInfo} for each provided action.
     * This is the "bulk" version of
     * {@link ActionTargetSelector#getTargetForAction(ActionDTO.Action)}.
     *
     * //TODO: provide a flag to cause an explanation/failure to be reported for unsupported actions
     * //where applicable.
     *
     * @param actions Stream of action recommendations.
     * @return Map of (action id) to ({@link ActionTargetInfo}) for the action, for each
     *         action in the input.
     */
    @Nonnull
    public Map<Long, ActionTargetInfo> getTargetsForActions(@Nonnull final Stream<ActionDTO.Action> actions) {
        final Map<Long, ActionResolutionData> resolutionStateByAction = actions
            .collect(Collectors.toMap(ActionDTO.Action::getId, ActionResolutionData::new));
        final Set<Long> selectedEntityIds = new HashSet<>();
        for (final ActionResolutionData resolutionState : resolutionStateByAction.values()) {
            try {
                // Select the entity to execute the action against, and put the result in the map
                final ActionEntity executantEntity = getExecutantEntity(resolutionState.recommendation);
                resolutionState.setExecutantEntity(executantEntity);
                selectedEntityIds.add(executantEntity.getId());
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

        try {
            // Get info about the targets and probes related to the entities
            // For performance reasons, this remote call is done in bulk
            // The Map is entityId -> EntityInfo
            final Map<Long, EntityInfo> entityInfos = getEntitiesInfo(selectedEntityIds);

            for (Entry<Long, ActionResolutionData> actionIdAndResolutionState : resolutionStateByAction.entrySet()) {
                final ActionResolutionData resolutionState = actionIdAndResolutionState.getValue();
                final ActionDTO.Action recommendation = resolutionState.recommendation;
                final ActionEntity executantEntity = resolutionState.getExecutantEntity();
                if (executantEntity != null) {
                    final EntityInfo selectedEntityInfo =
                        entityInfos.get(executantEntity.getId());
                    if (selectedEntityInfo != null) {
                        // Get an id (either a targetId or a probeId, depending on the provided function)
                        resolutionState.resolvedTargetInfo =
                            targetInfoResolver.getTargetInfoForAction(recommendation,
                                executantEntity, selectedEntityInfo);
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
         * Get the ID of the probe or target for the {@link ExecutableStep} of an {@link Action}.
         *
         * @param action TopologyProcessor Action
         * @param executantEntity an EntityInfo that provides target/probe data which will be used to
         *                        execute this action.
         * @return targetId for the action
         */
        @VisibleForTesting
        ActionTargetInfo getTargetInfoForAction(@Nonnull final ActionDTO.Action action,
                                                @Nonnull final ActionDTO.ActionEntity executantEntity,
                                                @Nonnull final EntityInfo executantEntityInfo) {
            final CachedCapabilities cachedCapabilities = probeCapabilityCache.getCachedCapabilities();
            SupportLevel bestSupportLevel = SupportLevel.UNSUPPORTED;
            Long bestTargetForSupportLevel = null;
            for (Entry<Long, Long> entry : executantEntityInfo.getTargetIdToProbeIdMap().entrySet()) {
                final Long targetId = entry.getKey();
                final Long probeId = entry.getValue();
                final SupportLevel targetSupportLevel =
                    cachedCapabilities.getSupportLevel(action, executantEntity, probeId);
                // Check for unknown explicitly because it's a lower index than all others.
                if (targetSupportLevel != SupportLevel.UNKNOWN &&
                    targetSupportLevel.getNumber() < bestSupportLevel.getNumber()) {
                    // Update the best support level encountered so far.
                    bestSupportLevel = targetSupportLevel;
                    // The first target encountered at the "better" support level is the best.
                    bestTargetForSupportLevel = targetId;
                } else if (targetSupportLevel == bestSupportLevel) {
                    final int curBestPriority = getTargetPriority(bestTargetForSupportLevel,
                        executantEntityInfo.getTargetIdToProbeIdMap(),
                        cachedCapabilities);
                    final int thisTargetPriority = getTargetPriority(targetId,
                        executantEntityInfo.getTargetIdToProbeIdMap(),
                        cachedCapabilities);
                    // A lower priority value is considered higher priority (because it's order
                    // in a descending list). If this target's priority is lower than the best
                    // so far, this should be the best one.
                    if (curBestPriority > thisTargetPriority) {
                        bestTargetForSupportLevel = targetId;
                    }
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
            return retBuilder.build();
        }

        private int getTargetPriority(@Nullable final Long targetId,
                                      @Nonnull final Map<Long, Long> targetIdToProbeIdMap,
                                      @Nonnull final CachedCapabilities cachedCapabilities) {
            if (targetId == null) {
                return UNSPECIFIED_PRIORITY;
            }

            // The targets should be present in the map, because we get the target values
            // from the map in the calling method. However, we check for nulls just in case to
            // avoid NPEs if the calling patterns change.
            Long probeId = targetIdToProbeIdMap.get(targetId);
            if (probeId == null) {
                logger.error("Unexpected null - no probe ID found for target ID {}", targetId);
                return UNSPECIFIED_PRIORITY;
            }

            return cachedCapabilities.getProbeCategory(probeId)
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
     * Get data about the targets and probes that discovered a Set of entities
     * For performance reasons, this remote call is done in bulk
     * The Map is entityId -> EntityInfo
     *
     * @param entityIds a Set of entity ids
     * @return a Map of entityId -> EntityInfo, containing data about the targets and probes that
     *  discovered the entity
     */
    private Map<Long, EntityInfoOuterClass.EntityInfo> getEntitiesInfo(Set<Long> entityIds) {
        Map<Long, EntityInfo> retMap = new HashMap<>(entityIds.size());
        entityServiceBlockingStub.getEntitiesInfo(
            EntityInfoOuterClass.GetEntitiesInfoRequest.newBuilder()
                .addAllEntityIds(entityIds)
                .build()).forEachRemaining(entityInfo -> {
                    retMap.put(entityInfo.getEntityId(), entityInfo);
        });
        return retMap;
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
