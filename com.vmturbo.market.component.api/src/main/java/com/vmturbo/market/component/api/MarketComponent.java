package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

/**
 * The client-side interface to the market component. This
 * is the only supported way of interacting with the market.
 */
public interface MarketComponent {

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

    /**
     * Registers a listener for price index notifications.
     *
     * @param listener listener to register
     */
    void addPriceIndexListener(@Nonnull final PriceIndexListener listener);

    /**
     * Register a listener for handling plan analysis topologies.
     *
     * @param listener the listener for plan analysis topologies
     */
    void addPlanAnalysisTopologyListener(@Nonnull final PlanAnalysisTopologyListener listener);
}
