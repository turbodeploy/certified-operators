package com.vmturbo.cost.component;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cost.component.entity.cost.PlanProjectedEntityCostStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;

/**
 * Responsible for cleaning data of deleted plans from the cost component.
 */
public class CostPlanGarbageCollector implements PlanGarbageCollector {
    private static final Logger logger = LogManager.getLogger();

    private final ActionContextRIBuyStore actionContextRIBuyStore;

    private final PlanProjectedEntityCostStore planProjectedEntityCostStore;

    private final PlanReservedInstanceStore planReservedInstanceStore;

    /**
     * Constructor.
     *
     * @param actionContextRIBuyStore Used to access RI buy action data.
     * @param planProjectedEntityCostStore Used to access projected entity costs.
     * @param planReservedInstanceStore Used to access projected RI data.
     */
    public CostPlanGarbageCollector(@Nonnull final ActionContextRIBuyStore actionContextRIBuyStore,
                                    @Nonnull final PlanProjectedEntityCostStore planProjectedEntityCostStore,
                                    @Nonnull final PlanReservedInstanceStore planReservedInstanceStore) {
        this.actionContextRIBuyStore = actionContextRIBuyStore;
        this.planProjectedEntityCostStore = planProjectedEntityCostStore;
        this.planReservedInstanceStore = planReservedInstanceStore;
    }

    @Nonnull
    @Override
    public List<ListExistingPlanIds> listPlansWithData() {
        return Arrays.asList(actionContextRIBuyStore::getContextsWithData,
            planReservedInstanceStore::getPlanIds,
            planProjectedEntityCostStore::getPlanIds);
    }

    @Override
    public void deletePlanData(final long planId) {
        try {
            actionContextRIBuyStore.deleteRIBuyContextData(planId);
        } catch (RuntimeException e) {
            logger.error("Failed to delete RI Buy context data for plan " + planId, e);
        }

        try {
            planProjectedEntityCostStore.deletePlanProjectedCosts(planId);
        } catch (RuntimeException e) {
            logger.error("Failed to delete plan projected costs for plan " + planId, e);
        }

        try {
            planReservedInstanceStore.deletePlanReservedInstanceStats(planId);
        } catch (RuntimeException e) {
            logger.error("Failed to delete plan reserved instance stats for plan " + planId, e);
        }

    }
}
