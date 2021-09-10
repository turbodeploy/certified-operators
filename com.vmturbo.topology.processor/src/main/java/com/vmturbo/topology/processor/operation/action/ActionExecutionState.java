package com.vmturbo.topology.processor.operation.action;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;

/**
 * This class represents action state when it is executed by a probe.
 */
public class ActionExecutionState {

    private final long actionId;
    private final ActionType actionType;

    private ActionResponseState actionState = ActionResponseState.IN_PROGRESS;
    private int progress = 0;
    private String description = "";

    /**
     * Constructor.
     *
     * @param actionId Action ID.
     * @param actionType Action type.
     */
    public ActionExecutionState(long actionId, ActionType actionType) {
        this.actionId = actionId;
        this.actionType = actionType;
    }

    public long getActionId() {
        return actionId;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public ActionResponseState getActionState() {
        return actionState;
    }

    public void setActionState(ActionResponseState actionState) {
        this.actionState = actionState;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    @Nonnull
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Get action status retrieved from {@link ActionState}.
     *
     * @return Action status or null if {@code ActionState} cannot be converted into {@code Status}.
     */
    @Nullable
    public Status getStatus() {
        switch (actionState) {
            case SUCCEEDED:
                return Status.SUCCESS;
            case FAILED:
                return Status.FAILED;
            case IN_PROGRESS:
                return Status.IN_PROGRESS;
            default:
                return null;
        }
    }
}
