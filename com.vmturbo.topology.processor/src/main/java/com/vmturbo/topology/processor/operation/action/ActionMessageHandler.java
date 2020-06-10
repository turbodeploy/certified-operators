package com.vmturbo.topology.processor.operation.action;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ActionProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResult;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Handles action execution responses from probes for a {@link Action} operation.
 */
public class ActionMessageHandler extends OperationMessageHandler<Action, ActionResult> {

    private final ActionOperationCallback callback;

    /**
     * Constructs action message handler.
     *
     * @param action operation to handle messages for
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to execute when operation response or error arrives
     */
    public ActionMessageHandler(@Nonnull final Action action,
                                @Nonnull final Clock clock,
                                final long timeoutMilliseconds,
            @Nonnull ActionOperationCallback callback) {
        super(action, clock, timeoutMilliseconds, callback);
        this.callback = Objects.requireNonNull(callback);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case ACTIONPROGRESS:
                callback.onActionProgress(receivedMessage.getActionProgress());
                return HandlerStatus.IN_PROGRESS;
            case ACTIONRESPONSE:
                callback.onSuccess(receivedMessage.getActionResponse());
                return HandlerStatus.COMPLETE;
            default:
                return super.onMessage(receivedMessage);
        }
    }

    /**
     * Action operation callback is used to receive respobses from the action execution.
     */
    public interface ActionOperationCallback extends OperationCallback<ActionResult> {
        /**
         * Called when action progress is reported by action operation.
         *
         * @param actionProgress action progress
         */
        void onActionProgress(@Nonnull ActionProgress actionProgress);
    }
}
