package com.vmturbo.action.orchestrator.execution;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.vmturbo.action.orchestrator.action.ActionTypeToActionTypeCaseConverter;
import com.vmturbo.action.orchestrator.store.ActionCapabilitiesStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;

/**
 * Class resolves conflicts when multiple targets are able to execute the action.
 * Resolving is based on priorities of probe categories which are hardcoded inside.
 */
public class ActionTargetByProbeCategoryResolver implements ActionTargetResolver {

    private final Logger logger = LogManager.getLogger();

    private final TopologyProcessor topologyProcessor;

    private final ActionCapabilitiesStore actionCapabilitiesStore;

    /**
     * If multiple probes support the same action, this priority is used to determine which target
     * will execute the action. Position of category in list determines priority. List contains
     * highest priority categories which should be executed firstly at the begin of it and lowest
     * priority categories at the end.
     **/
    private static final List<String> PROBE_CATEGORY_PRIORITIES =
            ImmutableList.of(
                    "CLOUD MANAGEMENT",
                    "LOAD BALANCER",
                    "STORAGE",
                    "FABRIC",
                    "HYPERVISOR",
                    "OPERATION MANAGER APPLIANCE",
                    "APPLICATION SERVER",
                    "DATABASE SERVER",
                    "FLOW",
                    "CUSTOM",
                    "GUEST OS PROCESSES");

    /** Provides Priorities of Probe categories for each action type. **/
    private static final Map<ActionTypeCase, List<String>> ACTION_TYPES_PROBE_PRIORITIES;

    /** For now we have the same priorities for all action types. **/
    static {
        final Builder<ActionTypeCase, List<String>> builder = ImmutableMap.builder();
        builder.put(ActionTypeCase.ACTIONTYPE_NOT_SET, PROBE_CATEGORY_PRIORITIES);
        builder.put(ActionTypeCase.MOVE, PROBE_CATEGORY_PRIORITIES);
        builder.put(ActionTypeCase.RECONFIGURE, PROBE_CATEGORY_PRIORITIES);
        builder.put(ActionTypeCase.PROVISION, PROBE_CATEGORY_PRIORITIES);
        builder.put(ActionTypeCase.RESIZE, PROBE_CATEGORY_PRIORITIES);
        builder.put(ActionTypeCase.ACTIVATE, PROBE_CATEGORY_PRIORITIES);
        builder.put(ActionTypeCase.DEACTIVATE, PROBE_CATEGORY_PRIORITIES);
        ACTION_TYPES_PROBE_PRIORITIES = builder.build();
    }

    public ActionTargetByProbeCategoryResolver(@Nonnull final TopologyProcessor topologyProcessor,
            @Nonnull final ActionCapabilitiesStore actionCapabilitiesStore) {
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.actionCapabilitiesStore = Objects.requireNonNull(actionCapabilitiesStore);
    }

    /**
     * Returns priorities for each probe category for the specified action type.
     *
     * @return probe priorities for action types
     */
    @Nonnull
    public static List<String> getProbePrioritiesFor(ActionTypeCase actionTypeCase) {
        final List<String> priorities = ACTION_TYPES_PROBE_PRIORITIES.get(actionTypeCase);
        if (priorities == null) {
            throw new IllegalArgumentException("There is not Probe Priorities for " + actionTypeCase);
        }
        return priorities;
    }

    /**
     * Resolves target for action which can be executed by multiple targets.
     * Resolving is based on probe category prorities which are hardcoded inside.
     *
     * @param action action to be executed.
     * @param targets targets which can execute the action
     * @return resolved targetId
     * @throws TargetResolutionException when either provided action was null or set of targets
     * was null or empty
     */
    @Override
    public long resolveExecutantTarget(@Nonnull ActionDTO.Action action, @Nonnull Set<Long> targets)
            throws TargetResolutionException {
        checkForNullActionAndTargets(action, targets);
        if (targets.size() == 1) {
            return targets.iterator().next();
        }
        logger.debug("There are multiple targets for action {}", action);
        final Map<Long, ProbeInfo> targetIdsToProbeInfos = getProbeInfosOfTargets(targets);
        final Map<Long, ProbeInfo> supportActionTargetsToProbes =
                getSupportActionTargetsToProbes(action, targetIdsToProbeInfos);
        if (supportActionTargetsToProbes.size() == 1) {
            return supportActionTargetsToProbes.keySet().iterator().next();
        }

        final List<String> probePriorities =
                ACTION_TYPES_PROBE_PRIORITIES.get(action.getInfo().getActionTypeCase());

        final Map<Long, Integer> targetIdsToPriorities = supportActionTargetsToProbes.entrySet().stream()
                .filter(targetIdProbe -> probePriorities.contains(getCategoryUppercase(targetIdProbe)))
                .collect(Collectors.toMap(targetIdProbe -> targetIdProbe.getKey(),
                        targetIdProbe -> probePriorities.indexOf(getCategoryUppercase(targetIdProbe))));
        return targetIdsToPriorities.entrySet().stream().min(Entry.comparingByValue()).get().getKey();
    }

    @Nonnull
    private Map<Long, ProbeInfo> getSupportActionTargetsToProbes(@Nonnull Action action,
            @Nonnull Map<Long, ProbeInfo> targetIdsToProbeInfos) {
        final Map<Long, List<ProbeActionCapability>> probesCapabilities =
                actionCapabilitiesStore.getCapabilitiesForProbes(targetIdsToProbeInfos.values()
                        .stream().map(ProbeInfo::getId).collect(Collectors.toSet()));
        final Set<Long> probesWhichNotSupportAction = getProbesWhichDoNotSupportAction(action
                .getInfo().getActionTypeCase(), probesCapabilities);
        return filterUnsupported
                (targetIdsToProbeInfos, probesWhichNotSupportAction);
    }

    @Nonnull
    private Set<Long> getProbesWhichDoNotSupportAction(@Nonnull ActionTypeCase actionType,
            @Nonnull Map<Long, List<ProbeActionCapability>> probesCapabilities) {
        final Set<Long> probesWhichNotSupportAction = new HashSet<>();
        for (Map.Entry<Long, List<ProbeActionCapability>> capabilitiesOfProbe :
                probesCapabilities.entrySet()) {
            if (!isActionSupportedByProbe(capabilitiesOfProbe.getValue(), actionType)) {
                probesWhichNotSupportAction.add(capabilitiesOfProbe.getKey());
            }
        }
        return probesWhichNotSupportAction;
    }

    private boolean isActionSupportedByProbe(@Nonnull List<ProbeActionCapability> capabilitiesOfProbe,
            @Nonnull ActionTypeCase actionType) {
        return capabilitiesOfProbe.stream().anyMatch(actionCapability ->
                isActionSupportedByCapability(actionCapability, actionType));
    }

    private boolean isActionSupportedByCapability(@Nonnull ProbeActionCapability actionCapability,
            @Nonnull ActionTypeCase actionType) {
        return actionCapability.getCapabilityElementList().stream().anyMatch(capabilityElement ->
                isActionSupportedByCapabilityElement(capabilityElement, actionType));
    }

    private boolean isActionSupportedByCapabilityElement(
            @Nonnull ActionCapabilityElement capabilityElement,
            @Nonnull ActionTypeCase actionType) {
        final ActionTypeCase capabilityActionType = ActionTypeToActionTypeCaseConverter
                .getActionTypeCaseFor(capabilityElement.getActionType());
        return capabilityActionType == actionType &&
                capabilityElement.getActionCapability() == ActionCapability.SUPPORTED;
    }

    private Map<Long, ProbeInfo> filterUnsupported(@Nonnull Map<Long, ProbeInfo> targetIdsToProbeInfos,
            @Nonnull Set<Long> unsupportedProbeIds) {
        final ImmutableMap.Builder<Long, ProbeInfo> builder = ImmutableMap.builder();
        for (Entry<Long, ProbeInfo> targetIdToProbe : targetIdsToProbeInfos.entrySet()) {
            if (!unsupportedProbeIds.contains(targetIdToProbe.getValue().getId())) {
                builder.put(targetIdToProbe);
            }
        }
        return builder.build();
    }

    private static String getCategoryUppercase(@Nonnull final Entry<Long, ProbeInfo> targetIdProbe) {
        return targetIdProbe.getValue().getCategory().toUpperCase();
    }

    @Nonnull
    private Map<Long, ProbeInfo> getProbeInfosOfTargets(@Nonnull Set<Long> targets) {
        final Map<Long, Optional<TargetInfo>> targetInfos = targets.stream()
                .collect(Collectors.toMap(Function.identity(), targetId -> getProbeOrTarget
                        (targetId, topologyProcessor::getTarget)));

        final Map<Long, Optional<ProbeInfo>> probeInfosOptionalsOfTargets = targetInfos.entrySet().stream()
                .filter(targetIdToTargetInfoOpt -> targetIdToTargetInfoOpt.getValue().isPresent())
                .collect(Collectors.toMap(targetIdToTargetInfoOpt -> targetIdToTargetInfoOpt.getKey(),
                        targetIdToTargetInfoOpt -> getProbeOrTarget(targetIdToTargetInfoOpt
                                .getValue().get().getProbeId(), topologyProcessor::getProbe)));

        return probeInfosOptionalsOfTargets
                .entrySet().stream().filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().get()));
    }

    private void checkForNullActionAndTargets(@Nonnull Action action, @Nonnull Set<Long> targets)
            throws TargetResolutionException {
        if (action == null || CollectionUtils.isEmpty(targets)) {
            throw new TargetResolutionException("Cannot resolve target for action");
        }
    }

    private <T> Optional<T> getProbeOrTarget(long id, GetByIdFunction<T> getTargetOrProbe) {
        try {
            return Optional.of(getTargetOrProbe.apply(id));
        } catch (CommunicationException | TopologyProcessorException e) {
            logger.warn("Cannot resolve Target or probe for Id:{}", id, e);
        }
        return Optional.empty();
    }

    /**
     * Interface of function which returnes generic type objects by id.
     *
     * @param <T> Type of object to be returned
     */
    @FunctionalInterface
    private interface GetByIdFunction<T> {
        /**
         * Returnes object by id.
         *
         * @param id id of object to be returned
         * @return object with this id
         * @throws CommunicationException if cannot send or receive data from TP
         * @throws TopologyProcessorException if TP cannot resolve the object
         */
        T apply(long id) throws CommunicationException, TopologyProcessorException;
    }
}
