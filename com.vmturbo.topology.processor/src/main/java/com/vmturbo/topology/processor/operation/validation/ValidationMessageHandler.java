package com.vmturbo.topology.processor.operation.validation;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.operation.IOperationManager.OperationCallback;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Handles validation responses from probes for a {@link Validation} operation.
 */
public class ValidationMessageHandler
        extends OperationMessageHandler<Validation, ValidationResponse> {

    /**
     * Constructs validation message handler.
     *
     * @param validation validation operation
     * @param clock clock to use for time operations
     * @param timeoutMilliseconds timeout value
     * @param callback callback to execute when operation response or error arrives
     */
    public ValidationMessageHandler(@Nonnull final Validation validation,
            @Nonnull final Clock clock, final long timeoutMilliseconds,
            @Nonnull OperationCallback<ValidationResponse> callback) {
        super(validation, clock, timeoutMilliseconds, callback);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case VALIDATIONRESPONSE:
                getCallback().onSuccess(receivedMessage.getValidationResponse());
                return HandlerStatus.COMPLETE;
            default:
                return super.onMessage(receivedMessage);
        }
    }
}
