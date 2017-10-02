package com.vmturbo.topology.processor.operation.action;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Handles action execution responses from probes for a {@link Action} operation.
 */
public class ActionMessageHandler extends OperationMessageHandler<Action> {

    public ActionMessageHandler(@Nonnull final OperationManager manager,
                                @Nonnull final Action action,
                                @Nonnull final Clock clock,
                                final long timeoutMilliseconds) {
        super(manager, action, clock, timeoutMilliseconds);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case ACTIONPROGRESS:
                manager.notifyProgress(operation, receivedMessage);
                return HandlerStatus.IN_PROGRESS;
            case ACTIONRESPONSE:
                manager.notifyActionResult(operation, receivedMessage.getActionResponse());
                return HandlerStatus.COMPLETE;
            default:
                return super.onMessage(receivedMessage);
        }
    }
}
