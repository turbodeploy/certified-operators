package com.vmturbo.topology.processor.operation.actionapproval;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage.MediationClientMessageCase;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Message handler for action state updates from external action approval backend.
 */
public class GetActionStateMessageHandler
        extends OperationMessageHandler<GetActionState, GetActionStateResponse> {

    /**
     * Constructs message handler for action state update from external approval backend.
     *
     * @param operation operation to handle messages for
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to execute when operation response or error arrives
     */
    public GetActionStateMessageHandler(@Nonnull GetActionState operation, @Nonnull Clock clock,
            long timeoutMilliseconds, @Nonnull OperationCallback<GetActionStateResponse> callback) {
        super(operation, clock, timeoutMilliseconds, callback);
    }

    @Nonnull
    @Override
    protected HandlerStatus onMessage(@Nonnull MediationClientMessage receivedMessage) {
        if (receivedMessage.getMediationClientMessageCase()
                == MediationClientMessageCase.ACTIONSTATESRESPONSE) {
            getCallback().onSuccess(receivedMessage.getActionStatesResponse());
            return HandlerStatus.COMPLETE;
        } else {
            return super.onMessage(receivedMessage);
        }
    }
}
