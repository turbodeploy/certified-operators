package com.vmturbo.plan.orchestrator.api.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.PlanDeleted;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.StatusUpdate;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.plan.orchestrator.api.PlanListener;

/**
 * Utility class to help components that store plan-specific data delete that data when a plan
 * is deleted in the action orchestrator. Defers to the embedded {@link PlanGarbageCollector}
 * to list the plan ids that the current component has data for, and to delete plan data for
 * a plan that no longer exists.
 *
 * <p/>There are two parts to the garbage detection and collection:
 * 1) When a plan is deleted, the plan orchestrator sends a notification.
 *    The {@link PlanGarbageDetector} listens to the notification, and notifies the embedded
 *    {@link PlanGarbageCollector} to delete plan data.
 * 2) At regular (configurable) intervals, query the plan orchestrator for currently existing plans,
 *    and delete data for all plan ids that don't exist in the plan orchestrator. This is mostly
 *    a safeguard in case 1) doesn't work properly.
 */
public class PlanGarbageDetector implements PlanListener {
    private static final Logger logger = LogManager.getLogger();

    private final PlanServiceBlockingStub planStub;

    private final PlanGarbageCollector planGarbageCollector;

    private final long realtimeTopologyContextId;

    PlanGarbageDetector(@Nonnull final PlanServiceBlockingStub planStub,
                       final long garbageCollectionInterval,
                       @Nonnull final TimeUnit garbageCollectionTimeUnit,
                       @Nonnull final ScheduledExecutorService scheduledExecutorService,
                       @Nonnull final PlanGarbageCollector planGarbageCollector,
                       final long realtimeTopologyContextId) {
        this.planStub = planStub;
        this.planGarbageCollector = planGarbageCollector;
        this.realtimeTopologyContextId = realtimeTopologyContextId;

        scheduledExecutorService.scheduleAtFixedRate(this::deleteInvalidPlans,
            garbageCollectionInterval,
            garbageCollectionInterval,
            garbageCollectionTimeUnit);
    }

    private void deleteInvalidPlans() {
        final Set<Long> allPlans = new HashSet<>();
        try {
            planStub.getAllPlans(GetPlansOptions.getDefaultInstance())
                .forEachRemaining(plan -> allPlans.add(plan.getPlanId()));
        } catch (StatusRuntimeException e) {
            // Plan orchestrator might be down.
            // We will try again on the next interval.
            logger.error("Failed to fetch current plans. Error: {}. Will retry later.", e.getMessage());
            return;
        }


        final Set<Long> existingPlanData = new HashSet<>();
        planGarbageCollector.listPlansWithData().forEach(listOperation -> {
            try {
                listOperation.getPlanIds().forEach(id -> {
                    if (id != realtimeTopologyContextId) {
                        existingPlanData.add(id);
                    }
                });
            } catch (Exception e) {
                logger.error("Failed to list existing plan ids.", e);
            }
        });

        if (existingPlanData.isEmpty()) {
            return;
        }

        existingPlanData.forEach(existingPlan -> {
            if (!allPlans.contains(existingPlan)) {
                try {
                    planGarbageCollector.deletePlanData(existingPlan);
                } catch (RuntimeException e) {
                    logger.warn("Failed to delete plan " + existingPlan, e);
                }
            }
        });
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final StatusUpdate planStatusUpdate) {
        // Irrelevant.
    }

    @Override
    public void onPlanDeleted(@Nonnull final PlanDeleted planDeleted) {
        final long planId = planDeleted.getPlanId();
        logger.info("Received plan deletion notification for plan {}. Deferring to collector: {}",
            planId, planGarbageCollector.getClass().getSimpleName());
        planGarbageCollector.deletePlanData(planDeleted.getPlanId());
        logger.info("Completed clearing plan data for plan {}", planId);
    }

    /**
     * Each component that stores some plan data can implement this interface. Allows querying
     * known plan ids, and deleting plan data by plan id.
     */
    public interface PlanGarbageCollector {
        /**
         * Get a list of operations which return plan ids the component knows about.
         * Using a list of functions to avoid repetitive code to combine plan ids from different
         * places in the component (e.g. if there are two different tables/abstractions to check).
         *
         * @return The list of {@link ListExistingPlanIds} operations to run to get plan ids
         * that have data associated with them.
         */
        @Nonnull
        List<ListExistingPlanIds> listPlansWithData();

        /**
         * Delete all data associated with a particular plan, which no longer exists in the system.
         *
         * @param planId The id of the deleted plan.
         */
        void deletePlanData(long planId);

        /**
         * A helper function to list existing plan ids.
         *
         * <p/>Used to avoid repetitive code combining in components where there are multiple places
         * to check for existing ids.
         */
        @FunctionalInterface
        interface ListExistingPlanIds {

            /**
             *  Get the plan ids a particular class/place in the component knows about.
             *
             * @return Set of plan ids.
             * @throws Exception To let the calling code handle exceptions.
             */
            Set<Long> getPlanIds() throws Exception;
        }
    }
}
