package com.vmturbo.topology.processor.operation.action;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionProgress;
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

    private final List<ActionExecutionState> actions;

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
     * @param actions List of actions from action execution operation.
     * @param probeId Probe ID.
     * @param targetId Target ID.
     * @param identityProvider Identity provider to create a unique ID.
     */
    public ActionList(
            @Nonnull final List<ActionExecutionState> actions,
            final long probeId,
            final long targetId,
            @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, ACTION_DURATION_SUMMARY);
        this.actions = actions;
    }

    public List<ActionExecutionState> getActions() {
        return actions;
    }

    public String getActionIdsString() {
        return actions.stream()
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
     * @param actionProgress Action progress message to extract progress information from.
     */
    public void updateProgress(final ActionProgress actionProgress) {
        final long actionId = actionProgress.getActionOid();
        for (int actionIndex = 0; actionIndex < actions.size(); actionIndex++) {
            final ActionExecutionState action = actions.get(actionIndex);
            if (action.getActionId() == actionId) {
                updateProgress(actionProgress.getResponse(), actionIndex);
                return;
            }
        }

        logger.error("Action {} not found in the ActionList: {}", actionId, toString());
    }

    /**
     * Updates action progress from the specified action progress message.
     *
     * @param actionResponse Action response message to extract progress information from.
     * @param actionIndex The index of the action in the action list.
     */
    public void updateProgress(final ActionResponse actionResponse, final int actionIndex) {
        if (actionIndex < 0 || actionIndex >= actions.size()) {
            logger.error("Wrong action index {} for the ActionList: {}", actionIndex, toString());
            return;
        }

        final ActionExecutionState action = actions.get(actionIndex);
        action.setActionState(action.getActionState());
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
        actions.forEach(action -> {
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
