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

}
