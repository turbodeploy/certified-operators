package com.vmturbo.extractor.action;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;

/**
 * Data object containing information about a completed action, received as a notification
 * and ingested by the {@link CompletedActionWriter}.
 */
public class ExecutedAction {
    private final long actionId;
    private final ActionSpec actionSpec;
    private final String message;

    ExecutedAction(final long actionId,
            @Nonnull final ActionSpec actionSpec,
            @Nonnull final  String message) {
        this.actionId = actionId;
        this.actionSpec = actionSpec;
        this.message = message;
    }

    public long getActionId() {
        return actionId;
    }

    public ActionSpec getActionSpec() {
        return actionSpec;
    }

    public String getMessage() {
        return message;
    }
}
