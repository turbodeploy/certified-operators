package com.vmturbo.plan.orchestrator.plan;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;

public class PlanInstanceCompletionListener implements PlanStatusListener {

    private final Logger logger = LogManager.getLogger();

    private final PlanInstanceQueue planInstanceQueue;


    public PlanInstanceCompletionListener(PlanInstanceQueue planInstanceQueue) {
        this.planInstanceQueue = planInstanceQueue;
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan)
            throws PlanStatusListenerException {
        if (plan.getStatus().equals(PlanStatus.SUCCEEDED) ||
                plan.getStatus().equals(PlanStatus.FAILED) ||
                plan.getStatus().equals(PlanStatus.STOPPED)) {

            logger.info("Plan instance {} has completed execution.", plan.getPlanId());
            // Run the next plan instance in queue if available.
            planInstanceQueue.runNextPlanInstance();
        }
    }
}
