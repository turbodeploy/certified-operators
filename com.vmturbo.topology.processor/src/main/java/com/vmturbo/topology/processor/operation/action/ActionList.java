package com.vmturbo.topology.processor.operation.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An action list operation on a target.
 */
public class ActionList extends Operation {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, ActionExecutionState> actionMap;

    /**
     * Collect data about actions durations.
     */
    public static final DataMetricSummary ACTION_DURATION_SUMMARY = DataMetricSummary.builder()
            .withName("tp_action_duration_seconds")
            .withHelp("Duration of an action in the Topology Processor.")
            .build()
            .register();

    /**
     * Collect data about status of executed actions such as failure and success.
     */
    public static final DataMetricCounter ACTION_STATUS_COUNTER = DataMetricCounter.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "completed_actions_total")
            .withHelp("Status of all completed actions.")
            .withLabelNames("type", "status")
            .build()
            .register();

    /**
     * Constructs action list operation.
     *
     * @param actionMap Map of actions indexed by action OID.
     * @param probeId Probe ID.
     * @param targetId Target ID.
     * @param identityProvider Identity provider to create a unique ID.
     */
    public ActionList(
            @Nonnull final Map<Long, ActionExecutionState> actionMap,
            final long probeId,
            final long targetId,
            @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, ACTION_DURATION_SUMMARY);
        this.actionMap = actionMap;
    }

    public List<ActionExecutionState> getActions() {
        return new ArrayList<>(actionMap.values());
    }

    public String getActionIdsString() {
        return actionMap.values().stream()
                .map(ActionExecutionState::getActionId)
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
        return String.format("Action list operation (execution step of actions %s): %s",
                getActionIdsString(), super.toString());
    }

    /**
     * Updates action progress from the specified action progress message.
     *
     * @param actionResponse Action response message to extract progress information from.
     */
    public void updateProgress(final ActionResponse actionResponse) {
        final long actionOid = actionResponse.getActionOid();
        final ActionExecutionState action = actionMap.get(actionOid);

        if (action == null) {
            logger.error("Cannot find action with OID {} in {}", actionOid, toString());
            return;
        }

        action.setActionState(actionResponse.getActionResponseState());
        action.setProgress(actionResponse.getProgress());
        action.setDescription(actionResponse.getResponseDescription());
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return ACTION_STATUS_COUNTER;
    }

    /**
     * Update metrics for Action.
     */
    @Override
    protected void completeOperation() {
        getDurationTimer().observe();
        actionMap.values().forEach(action -> {
            final Status status = action.getStatus();
            if (status != null) {
                final String actionType = action.getActionType().toString();
                getStatusCounter().labels(actionType, status.name()).increment();
            } else {
                logger.error("Cannot translate ActionState {} into operation Status",
                        action.getActionState());
            }
        });
    }
}
