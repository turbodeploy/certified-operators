package com.vmturbo.action.orchestrator.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;

public class ActionSupportResolver {

    private Logger logger = LogManager.getLogger();

    private ActionCapabilitiesStore actionCapabilitiesStore;

    private ActionExecutor actionExecutor;

    /**
     * Class which determines whether action executed by probe or not.
     *
     * @param actionCapabilitiesStore store to get action capabilities for probes
     * @param actionExecutor Action executor to determine entities of actions
     */
    public ActionSupportResolver(@Nonnull final ActionCapabilitiesStore actionCapabilitiesStore,
            @Nonnull final ActionExecutor actionExecutor) {
        this.actionCapabilitiesStore = actionCapabilitiesStore;
        this.actionExecutor = actionExecutor;
    }

    /**
     * Determines action capabilities of actions and sets value to isSupported field of each action.
     * @param actions actions to resolve capabilities
     * @return actions with setted isSupported fields
     */
    public Collection<Action> resolveActionsSupporting(Collection<Action> actions) {
        try {
            final Map<Action, Long> actionsProbes = actionExecutor.getProbeIdsForActions(actions);
            Set<Long> probeIds = new HashSet<>(actionsProbes.values());
            Map<Long, List<ProbeActionCapability>> probeCapabilities =
                    actionCapabilitiesStore.getCapabilitiesForProbes(probeIds);
            Map<Action, List<ProbeActionCapability>> actionsAndCapabilities =
                    actionsProbes.entrySet()
                            .stream()
                            .collect(Collectors.toMap(actionAndProbe -> actionAndProbe.getKey(),
                                    actionAndProbe -> probeCapabilities.get(
                                            actionAndProbe.getValue())));
            Set<Action> filteredForUiActions = new HashSet<>();
            actionsAndCapabilities.entrySet()
                    .stream()
                    .forEach(entry -> filteredForUiActions.add(resolveActionProbeSupport(entry)));
            return filteredForUiActions;
        } catch (TargetResolutionException | UnsupportedActionException ex) {
            logger.warn("Cannot resolve support level for request for " + actions.size() + " actions", ex);
            return actions;
        }
    }

    private Action resolveActionProbeSupport(Entry<Action, List<ProbeActionCapability>> entry) {
        if (CollectionUtils.isEmpty(entry.getValue())) {
            return entry.getKey();
        }
        for (ProbeActionCapability capability : entry.getValue()) {
            for (ActionCapabilityElement element : capability.getCapabilityElementList()) {
                if (isActionTypesMatchesCapabilityActionType(entry, element)) {
                    return setIfActionSupported(entry.getKey(), element);
                }
            }
        }
        return entry.getKey();
    }

    private Action setIfActionSupported(Action action,
            ActionCapabilityElement element) {
        return new Action(action, getSupportLevel(element));
    }

    private ActionDTO.Action.SupportLevel getSupportLevel(ActionCapabilityElement element) {
        if (element.getActionCapability() == ActionCapability.NOT_SUPPORTED) {
            return SupportLevel.UNSUPPORTED;
        } else if (element.getActionCapability() == ActionCapability.NOT_EXECUTABLE) {
            return SupportLevel.SHOW_ONLY;
        }
        return SupportLevel.SUPPORTED;
    }

    private boolean isActionTypesMatchesCapabilityActionType(
            Entry<Action, List<ProbeActionCapability>> entry, ActionCapabilityElement element) {
        return ActionDTOUtil.getActionInfoActionType(entry.getKey().getRecommendation()
                .getInfo()) == element.getActionType();
    }
}
