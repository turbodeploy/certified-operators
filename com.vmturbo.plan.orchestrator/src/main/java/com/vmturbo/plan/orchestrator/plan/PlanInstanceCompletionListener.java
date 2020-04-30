package com.vmturbo.plan.orchestrator.plan;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

public class PlanInstanceCompletionListener implements PlanStatusListener {

    private final Logger logger = LogManager.getLogger();

    private final PlanInstanceQueue planInstanceQueue;


    public PlanInstanceCompletionListener(PlanInstanceQueue planInstanceQueue) {
        this.planInstanceQueue = planInstanceQueue;
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan)
            throws PlanStatusListenerException {
        if (PlanDTOUtil.isTerminalStatus(plan.getStatus())) {
            logger.info("Plan instance {} has completed execution.", plan.getPlanId());
            // Run the next plan instance in queue if available.
            planInstanceQueue.runNextPlanInstance();
        }
    }

    @Override
    public void onPlanDeleted(@Nonnull final PlanInstance plan) {
        if (!PlanDTOUtil.isTerminalStatus(plan.getStatus())) {
            // Plan got deleted while running, so it's safe to run the next plan instance in queue.
            logger.info("Plan instance {} deleted (while in status {}). Queueing next instance.",
                plan.getPlanId(), plan.getStatus());
            logger.debug("Full plan instance: {}", plan);
            // Run the next plan instance in queue if available.
            planInstanceQueue.runNextPlanInstance();
        }
    }
}
