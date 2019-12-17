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
     * Registers a listener for projected entity cost notifications.
     *
     * @param listener Listener to register.
     */
    void addProjectedEntityCostsListener(@Nonnull final ProjectedEntityCostsListener listener);

    /**
     * Registers a listener for projected entity reserved instance coverage notifications.
     *
     * @param listener Listener to register.
     */
    void addProjectedEntityRiCoverageListener(@Nonnull final ProjectedReservedInstanceCoverageListener listener);

    /**
     * Register a listener for handling plan analysis topologies.
     *
     * @param listener the listener for plan analysis topologies
     */
    void addPlanAnalysisTopologyListener(@Nonnull final PlanAnalysisTopologyListener listener);

    /**
     * Register a listener for handling analysis summaries
     *
     * @param listener the listener for analysis summaries
     */
    void addAnalysisSummaryListener(@Nonnull final AnalysisSummaryListener listener);

    /**
     * Register a listener for handling analysis run status.
     *
     * @param listener the listener for analysis run status.
     */
    void addAnalysisStatusListener(@Nonnull AnalysisStatusNotificationListener listener);
}
