package com.vmturbo.topology.processor.operation.validation;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;

/**
 * Handles validation responses from probes for a {@link Discovery} operation.
 */
public class ValidationMessageHandler extends OperationMessageHandler<Validation> {

    public ValidationMessageHandler(@Nonnull final OperationManager manager,
                                    @Nonnull final Validation validation,
                                    @Nonnull final Clock clock,
                                    final long timeoutMilliseconds) {
        super(manager, validation, clock, timeoutMilliseconds);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case VALIDATIONRESPONSE:
                manager.notifyValidationResult(operation, receivedMessage.getValidationResponse());
                return HandlerStatus.COMPLETE;
            default:
                return super.onMessage(receivedMessage);
        }
    }
}
