package com.vmturbo.topology.processor.operation.action;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An action operation on a target.
 */
public class Action extends Operation {

    /**
     * This is the  instance ID of the over-arching action
     * from the Action Orchestrator. It's distinct
     * from the ID of this {@link Action} operation. Multi-step
     * actions will have multiple {@link Action}s, each with their
     * own operation ID, but all with a common actionId.
     */
    private final long actionInstanceId;

    /**
     * The action stable id. This will be included in action updates as that instance
     * of the action may not be around when the component receives the update.
     */
    private final long actionStableId;

    private int progress = 0;
    private String description = "";
    private final ActionType actionType;

    /**
     * Constructs action operation.
     *
     * @param actionInstanceId action id
     * @param actionStableId action stable id
     * @param probeId probe OID
     * @param targetId target OID
     * @param identityProvider identity provider to create a unique ID
     * @param actionType action type
     */
    public Action(final long actionInstanceId,
                  final long actionStableId,
                  final long probeId,
                  final long targetId,
                  @Nonnull final IdentityProvider identityProvider,
                  @Nonnull ActionType actionType) {
        super(probeId, targetId, identityProvider, ActionList.ACTION_DURATION_SUMMARY);
        this.actionInstanceId = actionInstanceId;
        this.actionStableId = actionStableId;
        this.actionType = actionType;
    }

    @Override
    public String toString() {
        return "Action operation (execution step of action " + actionInstanceId + "): " + super.toString();
    }

    /**
     * Updates action progress from the specified action response object.
     *
     * @param actionResponse action response to extract progress information from
     */
    public void updateProgress(final ActionResponse actionResponse) {
        this.progress = actionResponse.getProgress();
        this.description = actionResponse.getResponseDescription();
    }

    public long getActionInstanceId() {
        return actionInstanceId;
    }

    public long getActionStableId() {
        return actionStableId;
    }

    public int getProgress() {
        return progress;
    }

    @Nonnull
    public String getDescription() {
        return description;
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return ActionList.ACTION_STATUS_COUNTER;
    }

    /**
     * Update metrics for Action.
     */
    @Override
    protected void completeOperation() {
        getDurationTimer().observe();
        getStatusCounter().labels(actionType.toString(), getStatus().name()).increment();
    }

    public ActionType getActionType() {
        return actionType;
    }
}
