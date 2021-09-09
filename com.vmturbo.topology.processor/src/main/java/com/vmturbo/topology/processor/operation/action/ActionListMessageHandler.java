package com.vmturbo.topology.processor.operation.action;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ActionListResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionProgress;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Handles action list execution responses from probes for a {@link ActionList} operation.
 */
public class ActionListMessageHandler extends OperationMessageHandler<ActionList, ActionListResponse> {

    private final ActionListOperationCallback callback;

    /**
     * Constructs action list message handler.
     *
     * @param actionList Action list operation to handle messages for.
     * @param clock Clock to use for time operations.
     * @param timeoutMilliseconds Timeout value in milliseconds.
     * @param callback Callback to execute when operation response or error arrives.
     */
    public ActionListMessageHandler(
            @Nonnull final ActionList actionList,
            @Nonnull final Clock clock,
            final long timeoutMilliseconds,
            @Nonnull final ActionListOperationCallback callback) {
        super(actionList, clock, timeoutMilliseconds, callback);
        this.callback = Objects.requireNonNull(callback);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case ACTIONPROGRESS:
                callback.onActionProgress(receivedMessage.getActionProgress());
                return HandlerStatus.IN_PROGRESS;
            case ACTIONLISTRESPONSE:
                callback.onSuccess(receivedMessage.getActionListResponse());
                return HandlerStatus.COMPLETE;
            default:
                return super.onMessage(receivedMessage);
        }
    }

    /**
     * Action list operation callback is used to receive responses from the action list execution.
     */
    public interface ActionListOperationCallback extends OperationCallback<ActionListResponse> {
        /**
         * Called when action progress is reported by action list operation.
         *
         * @param actionProgress Action progress.
         */
        void onActionProgress(@Nonnull ActionProgress actionProgress);
    }
}
