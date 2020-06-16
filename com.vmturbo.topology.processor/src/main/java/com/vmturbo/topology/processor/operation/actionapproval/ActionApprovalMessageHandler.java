package com.vmturbo.topology.processor.operation.actionapproval;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage.MediationClientMessageCase;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Message handler for sending action approvals to external action approval backend.
 */
public class ActionApprovalMessageHandler
        extends OperationMessageHandler<ActionApproval, ActionApprovalResponse> {

    /**
     * Constructs action approval message handler.
     *
     * @param operation operation to handle messages for
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to execute when operation response or error arrives
     */
    public ActionApprovalMessageHandler(@Nonnull ActionApproval operation, @Nonnull Clock clock,
            long timeoutMilliseconds, @Nonnull OperationCallback<ActionApprovalResponse> callback) {
        super(operation, clock, timeoutMilliseconds, callback);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        if (receivedMessage.getMediationClientMessageCase()
                == MediationClientMessageCase.ACTIONAPPROVALRESPONSE) {
            getCallback().onSuccess(receivedMessage.getActionApprovalResponse());
            return HandlerStatus.COMPLETE;
        } else {
            return super.onMessage(receivedMessage);
        }
    }
}
