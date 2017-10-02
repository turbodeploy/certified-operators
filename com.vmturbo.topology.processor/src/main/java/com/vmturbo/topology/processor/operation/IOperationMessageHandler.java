package com.vmturbo.topology.processor.operation;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.communication.ExpiringMessageHandler;

/**
 * Interface for operation message handlers.
 */
public interface IOperationMessageHandler<O extends Operation> extends ExpiringMessageHandler {
    /**
     * Return the operation object handled.
     *
     * @return operation object
     */
    @Nonnull
    O getOperation();
}
