package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

/**
 * Plan orchestrator remote client.
 */
public interface PlanOrchestrator {

    /**
     * Add {@link PlanListener} to the Plan Orchestrator.
     *
     * @param planListener the plan listener.
     */
    void addPlanListener(@Nonnull PlanListener planListener);

    /**
     * Add {@link PlanExportListener} to the Plan Orchestrator.
     *
     * @param planExportListener the plan export listener.
     */
    void addPlanExportListener(@Nonnull PlanExportListener planExportListener);

    /**
     * Add {@link ReservationListener} to Plan Orchestrator.
     *
     * @param reservationListener the reservation listener.
     */
    void addReservationListener(@Nonnull ReservationListener reservationListener);
}
