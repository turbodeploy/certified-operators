package com.vmturbo.topology.processor.operation.actionapproval;

import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage.MediationClientMessageCase;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Message handler for sending action state updates to external approval backend.
 */
public class ActionUpdateStateMessageHandler
        extends OperationMessageHandler<ActionUpdateState, ActionErrorsResponse> {

    /**
     * Constructs external action state update message handler.
     *
     * @param operation operation to handle messages for
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to execute when operation response or error arrives
     */
    public ActionUpdateStateMessageHandler(@Nonnull ActionUpdateState operation,
            @Nonnull Clock clock, long timeoutMilliseconds,
            @Nonnull OperationCallback<ActionErrorsResponse> callback) {
        super(operation, clock, timeoutMilliseconds, callback);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        if (receivedMessage.getMediationClientMessageCase()
                == MediationClientMessageCase.ACTIONERRORSRESPONSE) {
            onResult(receivedMessage.getActionErrorsResponse());
            return HandlerStatus.COMPLETE;
        } else {
            return super.onMessage(receivedMessage);
        }
    }

    private void onResult(@Nonnull ActionErrorsResponse response) {
        final List<ErrorDTO> errors = response.getErrorsList()
                .stream()
                .map(actionError -> ErrorDTO.newBuilder().setEntityUuid(
                        Long.toString(actionError.getActionOid())).setDescription(
                        actionError.getMessage()).setSeverity(ErrorSeverity.CRITICAL).build())
                .collect(Collectors.toList());
        operation.addErrors(errors);
        getCallback().onSuccess(response);
    }
}
