package com.vmturbo.cost.api;

import javax.annotation.Nonnull;

/**
 * The client interface to a remote cost component.
 */
public interface CostComponent {

    /**
     * Registers a listener for cost notifications.
     *
     * @param listener The listener to register.
     */
    void addCostNotificationListener(@Nonnull CostNotificationListener listener);

    /**
     * Registers a listener for topology cost chunks.
     *
     * @param listener the listener to register
     */
    void addTopologyCostListener(@Nonnull TopologyCostListener listener);
}
