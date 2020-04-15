package com.vmturbo.action.orchestrator.store;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;

/**
 * Responsible for cleaning data of deleted plans from the action orchestrator component.
 */
public class ActionPlanGarbageCollector implements PlanGarbageCollector {
    private static final Logger logger = LogManager.getLogger();

    private final ActionStorehouse actionStorehouse;

    /**
     * Constructor.
     *
     * @param actionStorehouse Contains methods to list/delete plan data.
     */
    public ActionPlanGarbageCollector(@Nonnull final ActionStorehouse actionStorehouse) {
        this.actionStorehouse = actionStorehouse;
    }

    @Nonnull
    @Override
    public List<ListExistingPlanIds> listPlansWithData() {
        return Collections.singletonList(() -> actionStorehouse.getAllStores().keySet());
    }

    @Override
    public void deletePlanData(final long planId) {
        try {
            actionStorehouse.deleteStore(planId);
        } catch (StoreDeletionException e) {
            logger.error("Failed to delete plan data for plan: " + planId, e);
        }
    }
}
