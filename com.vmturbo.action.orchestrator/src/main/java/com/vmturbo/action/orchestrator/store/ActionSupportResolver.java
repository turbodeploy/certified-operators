package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.EntitiesResolutionException;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;

/**
 * Class is responsible for action support resolving, based on probe-supplied action capabilities.
 */
public class ActionSupportResolver {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A store to get action capabilities for probes
     */
    private final ActionCapabilitiesStore actionCapabilitiesStore;

    /**
     * For determining which probe would execute an action
     * Used in determining action support level
     */
    private final ActionTargetSelector actionTargetSelector;

    /**
     * Class which determines whether action executed by probe or not.
     *
     * @param actionCapabilitiesStore store to get action capabilities for probes
     * @param actionTargetSelector for determining which probe would execute an action
     */
    public ActionSupportResolver(@Nonnull final ActionCapabilitiesStore actionCapabilitiesStore,
                                 @Nonnull final ActionTargetSelector actionTargetSelector) {
        this.actionCapabilitiesStore = Objects.requireNonNull(actionCapabilitiesStore);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
    }

    /**
     * Determines action capabilities of actions and sets value to isSupported field of each action.
     *
     * Note that if multiple SDK action types map to the same XL action type the conflict must be resolved.
     * Currently, multiple conflicting action types are resolved by taking the MINIMUM support level.
     * So for example, if SUSPEND and TERMINATE both map to DEACTIVATE, and a probe specifies
     * NOT_SUPPORTED for SUSPEND and SUPPORTED for TERMINATE, then DEACTIVATE actions will be treated
     * as not supported for the probe.
     *
     * @param actions actions to resolve capabilities
     * @return actions with their isSupported field set.
     */
    public Collection<Action> resolveActionsSupporting(Collection<Action> actions) {
        final Map<Action, Long> actionsProbes = actionTargetSelector.getProbeIdsForActions(actions);
        final Map<Long, List<ProbeActionCapability>> probeCapabilities =
                actionCapabilitiesStore.getCapabilitiesForProbes(actionsProbes.values());
        final Map<Action, List<ProbeActionCapability>> actionsAndCapabilities =
                actionsProbes.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Entry::getKey,
                                actionAndProbe -> probeCapabilities.get(
                                        actionAndProbe.getValue())));

        return actionsAndCapabilities.entrySet().stream()
                .map(entry -> resolveActionProbeSupport(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }

    @Nonnull
    private Action resolveActionProbeSupport(@Nonnull Action action,
            @Nonnull List<ProbeActionCapability> probeCapabilities) {
        final Collection<ActionCapabilityElement> activeCapabilities =
                new CapabilityMatcher().getCapabilities(
                        action.getRecommendation(), probeCapabilities);
        final Optional<ActionCapability> capabilityLevel = activeCapabilities.stream()
                .map(ActionCapabilityElement::getActionCapability)
                .min(Comparator.comparing(ActionCapability::getNumber));
        return setActionSupportLevel(action,
            // If the probe has not specified a support level, use a default value of NOT_EXECUTABLE
            // (that is, show the action in the UI but do not permit execution).
            capabilityLevel.orElse(ActionCapability.NOT_EXECUTABLE));
    }

    @Nonnull
    private Action setActionSupportLevel(Action action, ActionCapability capability) {
        final SupportLevel newLevel = getSupportLevel(capability);
        if (newLevel == action.getSupportLevel()) {
            return action;
        } else {
            return new Action(action, newLevel);
        }
    }

    @Nonnull
    private static ActionDTO.Action.SupportLevel getSupportLevel(@Nonnull ActionCapability capability) {
        switch (capability) {
            case NOT_SUPPORTED:
                return SupportLevel.UNSUPPORTED;
            case NOT_EXECUTABLE:
                return SupportLevel.SHOW_ONLY;
            default:
                return SupportLevel.SUPPORTED;
        }
    }

    /**
     * Interface of a matcher, which selects all the capability elements, that are related to the
     * action specified in order.
     */
    private interface ICapabilityMatcher {
        @Nonnull
        Collection<ActionCapabilityElement> getCapabilities(@Nonnull ActionDTO.Action action,
                @Nonnull Collection<ProbeActionCapability> actionCapabilities);
    }

    /**
     * Abstract capability matcher, holding most of the matcher logic, that could be shared between
     * capability matchers.
     */
    private static class CapabilityMatcher implements ICapabilityMatcher {
        @Nonnull
        @Override
        public Collection<ActionCapabilityElement> getCapabilities(@Nonnull ActionDTO.Action action,
                @Nonnull Collection<ProbeActionCapability> actionCapabilities) {
            try {
                int entityType = ActionDTOUtil.getPrimaryEntity(action).getType();
                final Optional<ProbeActionCapability> capability = actionCapabilities.stream()
                        .filter(cpb -> cpb.getEntityType() == entityType)
                        .findAny();
                return capability.map(cpb -> cpb.getCapabilityElementList()
                        .stream()
                        .filter(cpbElement -> matches(action, cpbElement))
                        .collect(Collectors.toList())).orElse(Collections.emptyList());
            } catch (UnsupportedActionException e) {
                logger.error(("Error getting capability for action " + action), e);
                return Collections.emptyList();
            }
        }

        /**
         * Returns whether an action specified matches the action capability specified.
         *
         * @param action action to test
         * @param actionCapabilityElement action capability to test
         * @return whether the action and the capability match each other
         */
        private boolean matches(@Nonnull ActionDTO.Action action,
                @Nonnull ActionCapabilityElement actionCapabilityElement) {
            boolean match = ActionDTOUtil.getActionInfoActionType(action) ==
                    actionCapabilityElement.getActionType();

            // For a Move action, we need to check that the destination type is supported by the
            // probe
            if (match && ActionDTOUtil.getActionInfoActionType(action) == ActionType.MOVE) {
                if (!(actionCapabilityElement.hasMove() && action.getInfo().hasMove())) {
                    match = false;
                } else {
                    final Set<Integer> actionTargetEntityTypes = action.getInfo()
                            .getMove()
                            .getChangesList()
                            .stream()
                            .map(cp -> cp.getDestination().getType())
                            .collect(Collectors.toSet());
                    match = actionCapabilityElement.getMove()
                            .getTargetEntityTypeList()
                            .stream()
                            .anyMatch(actionTargetEntityTypes::contains);
                }
            }
            return match;
        }
    }
}
