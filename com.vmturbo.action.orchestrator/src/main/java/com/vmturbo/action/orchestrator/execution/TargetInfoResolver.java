package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache.CachedCapabilities;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Target info resolver.
 */
public class TargetInfoResolver {

    /**
     * If multiple probes support the same action, this priority is used to determine which target
     * will execute the action. Position of category in list determines priority. List contains
     * highest priority categories which should be executed firstly at the begin of it and lowest
     * priority categories at the end.
     **/
    private static final List<ProbeCategory> PROBE_CATEGORY_PRIORITIES =
            ImmutableList.of(ProbeCategory.CLOUD_NATIVE,
                    ProbeCategory.CLOUD_MANAGEMENT,
                    ProbeCategory.STORAGE,
                    ProbeCategory.FABRIC,
                    ProbeCategory.HYPERVISOR,
                    ProbeCategory.APPLICATION_SERVER,
                    ProbeCategory.DATABASE_SERVER,
                    ProbeCategory.CUSTOM,
                    ProbeCategory.GUEST_OS_PROCESSES);

    private static final int NO_CATEGORY_PRIORITY = PROBE_CATEGORY_PRIORITIES.size() + 1;

    private static final Logger logger = LogManager.getLogger();

    private static final ImmutableActionTargetInfo UNSUPPORTED =
            ImmutableActionTargetInfo.builder().supportingLevel(SupportLevel.UNSUPPORTED).build();

    private final ProbeCapabilityCache probeCapabilityCache;
    private final ActionConstraintStoreFactory actionConstraintStoreFactory;
    private final ActionExecutionEntitySelector actionExecutionEntitySelector;

    /**
     * Constructor.
     *
     * @param probeCapabilityCache the {@link ProbeCapabilityCache}
     * @param actionConstraintStoreFactory the {@link ActionConstraintStoreFactory}
     * @param actionExecutionEntitySelector the {@link ActionConstraintStoreFactory}
     */
    public TargetInfoResolver(@Nonnull final ProbeCapabilityCache probeCapabilityCache,
            @Nonnull final ActionConstraintStoreFactory actionConstraintStoreFactory,
            @Nonnull final ActionExecutionEntitySelector actionExecutionEntitySelector) {
        this.probeCapabilityCache = Objects.requireNonNull(probeCapabilityCache);
        this.actionConstraintStoreFactory = Objects.requireNonNull(actionConstraintStoreFactory);
        this.actionExecutionEntitySelector = Objects.requireNonNull(actionExecutionEntitySelector);
    }

    /**
     * Get the target information that will perform the action.
     *
     * @param action the {@link Action}
     * @param actionPartialEntityMap mapping entity oid to {@link ActionPartialEntity}
     * @param snapshot the {@link EntitiesAndSettingsSnapshot}
     * @return the {@link ActionTargetInfo}
     */
    @Nonnull
    public ActionTargetInfo getTargetInfoForAction(@Nonnull ActionDTO.Action action,
            @Nonnull Map<Long, ActionPartialEntity> actionPartialEntityMap,
            @Nonnull EntitiesAndSettingsSnapshot snapshot) {
        try {
            final ActionEntity executantEntity = getExecutantEntity(action);
            final ActionPartialEntity actionPartialEntity =
                    actionPartialEntityMap.get(executantEntity.getId());
            if (actionPartialEntity != null) {
                return getTargetInfoForAction(action, executantEntity, actionPartialEntity,
                        actionPartialEntityMap, snapshot);
            } else {
                logger.warn("Selected entity {} involved in action {} no longer present in system",
                        executantEntity::getId, action::getId);
            }
        } catch (EntitiesResolutionException | UnsupportedActionException e) {
            // If there was a problem determining the execution target for this action, log the
            // exception and mark the action as unsupported.
            // TODO (roman, March 18 2019): Should we exclude these actions from the output
            // entirely? For now we leave it up to the caller to filter out unsupported actions
            // if they don't care about them.
            logger.warn("An entity could not be selected for executing this action. action id: {}",
                    action.getId());
            logger.debug(e.getMessage(), e);
        }
        return UNSUPPORTED;
    }

    @Nonnull
    private ActionTargetInfo getTargetInfoForAction(@Nonnull ActionDTO.Action action,
            @Nonnull ActionEntity executantEntity, @Nonnull ActionPartialEntity actionPartialEntity,
            @Nonnull Map<Long, ActionPartialEntity> actionPartialEntityMap,
            @Nonnull EntitiesAndSettingsSnapshot snapshot) {
        final CachedCapabilities cachedCapabilities = probeCapabilityCache.getCachedCapabilities();
        final List<TargetInfo> targetInfos = new ArrayList<>();
        for (Long discoveringTargetId : actionPartialEntity.getDiscoveringTargetIdsList()) {
            cachedCapabilities.getProbeFromTarget(discoveringTargetId).ifPresent(probeId -> {
                final MergedActionCapability mergedActionCapability = cachedCapabilities
                        .getMergedActionCapability(action, executantEntity, probeId);
                // Explicit skipping of a target with an unknown support level for action.
                if (mergedActionCapability.getSupportLevel() == SupportLevel.UNKNOWN) {
                    return;
                }
                final ProbeCategory probeCategory =
                        cachedCapabilities.getProbeCategory(probeId).orElse(null);
                int priority = PROBE_CATEGORY_PRIORITIES.indexOf(probeCategory);
                priority = priority >= 0 ? priority : NO_CATEGORY_PRIORITY;
                targetInfos.add(new TargetInfo(
                        discoveringTargetId,
                        probeCategory,
                        mergedActionCapability.getSupportLevel(),
                        priority,
                        mergedActionCapability.getDisruptive(),
                        mergedActionCapability.getReversible()));
            });
        }

        Stream<TargetInfo> targetInfoStream = targetInfos.stream();

        // For a move action, if the target entity has more than 1 discovering target that supports
        // execution of the current action, then it is necessary to intersect the target and source
        // discovering targets. This check is required for shared entities that were discovered
        // from 2 targets, and for which the target that will be selected as primary for the action
        // execution is important.
        if (action.hasInfo() && action.getInfo().hasMove() && targetInfos.stream()
                .filter(targetInfo -> targetInfo.getSupportLevel() == SupportLevel.SUPPORTED)
                .limit(2)
                .count() > 1) {
            final Iterator<List<Long>> discoveringTargetIdsIterator = action.getInfo()
                    .getMove()
                    .getChangesList()
                    .stream()
                    .map(changeProvider -> changeProvider.getSource().getId())
                    .map(actionPartialEntityMap::get)
                    .filter(Objects::nonNull)
                    .map(ActionPartialEntity::getDiscoveringTargetIdsList)
                    .iterator();
            // Trying to find a common target that discovered the target and source entities.
            if (discoveringTargetIdsIterator.hasNext()) {
                final Set<Long> intersection = new HashSet<>(discoveringTargetIdsIterator.next());
                while (discoveringTargetIdsIterator.hasNext()) {
                    intersection.retainAll(new HashSet<>(discoveringTargetIdsIterator.next()));
                }
                if (!intersection.isEmpty()) {
                    targetInfoStream = targetInfoStream.filter(
                            info -> intersection.contains(info.getTargetId()));
                }
            }
        }

        return targetInfoStream.min(
                Comparator.comparingInt((TargetInfo lhs) -> lhs.getSupportLevel().getNumber())
                        .thenComparingInt(TargetInfo::getPriority)).map(targetInfo -> {
            final ImmutableActionTargetInfo.Builder actionTargetInfoBuilder =
                    ImmutableActionTargetInfo.builder()
                            .targetId(targetInfo.getTargetId())
                            .supportingLevel(targetInfo.getSupportLevel())
                            .disruptive(targetInfo.getDisruptive())
                            .reversible(targetInfo.getReversible());
            if (targetInfo.getProbeCategory() != null) {
                actionTargetInfoBuilder.addAllPrerequisites(
                        PrerequisiteCalculator.calculatePrerequisites(action, actionPartialEntity,
                                snapshot, targetInfo.getProbeCategory(),
                                actionConstraintStoreFactory));
            }
            return actionTargetInfoBuilder.build();
        }).orElse(UNSUPPORTED);
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
     * resolve in TopologyProcessor
     * @throws UnsupportedActionException if the type of the action is not supported
     */
    private ActionEntity getExecutantEntity(@Nonnull ActionDTO.Action action)
            throws EntitiesResolutionException, UnsupportedActionException {
        // Check for special cases using the entitySelector
        return actionExecutionEntitySelector.getEntity(action)
                .orElseThrow(() -> new EntitiesResolutionException(
                        "No entity could be found for this action: " + action.toString()));
    }

    /**
     * A helper class that stores information for deciding which target will perform the action.
     */
    private static class TargetInfo {
        private final long targetId;
        private final ProbeCategory probeCategory;
        private final SupportLevel supportLevel;
        private final int priority;
        private final Boolean disruptive;
        private final Boolean reversible;

        TargetInfo(
                long targetId,
                @Nullable ProbeCategory probeCategory,
                @Nonnull SupportLevel supportLevel,
                int priority,
                @Nullable Boolean disruptive,
                @Nullable Boolean reversible) {
            this.targetId = targetId;
            this.supportLevel = supportLevel;
            this.probeCategory = probeCategory;
            this.priority = priority;
            this.disruptive = disruptive;
            this.reversible = reversible;
        }

        public long getTargetId() {
            return targetId;
        }

        @Nonnull
        public SupportLevel getSupportLevel() {
            return supportLevel;
        }

        @Nullable
        public ProbeCategory getProbeCategory() {
            return probeCategory;
        }

        public int getPriority() {
            return priority;
        }

        @Nullable
        public Boolean getDisruptive() {
            return disruptive;
        }

        @Nullable
        public Boolean getReversible() {
            return reversible;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TargetInfo that = (TargetInfo)o;
            return targetId == that.targetId
                    && priority == that.priority
                    && probeCategory == that.probeCategory
                    && supportLevel == that.supportLevel
                    && Objects.equals(disruptive, that.disruptive)
                    && Objects.equals(reversible, that.reversible);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetId);
        }
    }
}