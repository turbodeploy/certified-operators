package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

/**
 * The client-side interface to the market component. This
 * is the only supported way of interacting with the market.
 */
public interface MarketComponent extends AutoCloseable {

    /**
     * Registers a listener for action recommendations.
     *
     * @param listener Listener to register.
     */
    void addActionsListener(@Nonnull final ActionsListener listener);

    /**
     * Registers a listener for topology notifications.
     *
     * @param listener Listener to register.
     */
    void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener);
}
