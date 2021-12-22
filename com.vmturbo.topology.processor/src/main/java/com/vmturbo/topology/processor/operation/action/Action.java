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
     * This is the ID of the over-arching action
     * from the Action Orchestrator. It's distinct
     * from the ID of this {@link Action} operation. Multi-step
     * actions will have multiple {@link Action}s, each with their
     * own operation ID, but all with a common actionId.
     */
    private final long actionId;

    private int progress = 0;
    private String description = "";
    private final ActionType actionType;

    /**
     * Constructs action operation.
     *
     * @param actionId action id
     * @param probeId probe OID
     * @param targetId target OID
     * @param identityProvider identity provider to create a unique ID
     * @param actionType action type
     */
    public Action(final long actionId,
                  final long probeId,
                  final long targetId,
                  @Nonnull final IdentityProvider identityProvider,
                  @Nonnull ActionType actionType) {
        super(probeId, targetId, identityProvider, ActionList.ACTION_DURATION_SUMMARY);
        this.actionId = actionId;
        this.actionType = actionType;
    }

    @Override
    public String toString() {
        return "Action operation (execution step of action " + actionId + "): " + super.toString();
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

    public long getActionId() {
        return actionId;
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
