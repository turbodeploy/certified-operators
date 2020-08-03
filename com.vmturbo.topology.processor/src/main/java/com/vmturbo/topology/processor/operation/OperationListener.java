package com.vmturbo.topology.processor.operation;

import javax.annotation.Nonnull;

/**
 * Implementation will receive operation-related notifications.
 */
public interface OperationListener {
    /**
     * Called when operation state changed.
     *
     * @param operation operation which state is notified
     */
    void notifyOperationState(@Nonnull final Operation operation);

    /**
     * Called when operations are cleared - there will be no more state updates for any
     * previously known operations.
     */
    void notifyOperationsCleared();

}
