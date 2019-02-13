package com.vmturbo.common.protobuf.action;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Exception thrown when a {@link ActionDTO.Action} is not supported at some point in processing.
 */
public class UnsupportedActionException extends Exception {

    /**
     * The action that triggered the exception.
     */
    private final ActionInfo actionInfo;
    private final long actionId;

    public UnsupportedActionException(long actionId, @Nonnull final ActionInfo actionInfo) {
        super("Action: " + actionId + " with type " + actionInfo.getActionTypeCase() +
            " is not supported.");
        this.actionId = actionId;
        this.actionInfo = actionInfo;
    }

    public UnsupportedActionException(@Nonnull final Action action) {
        this(action.getId(), action.getInfo());
    }

    public ActionTypeCase getActionType() {
        return actionInfo.getActionTypeCase();
    }

    public long getActionId() {
        return actionId;
    }
}
