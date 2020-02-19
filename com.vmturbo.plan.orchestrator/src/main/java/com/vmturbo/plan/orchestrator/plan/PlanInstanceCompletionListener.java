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

            try {
                logger.info("Plan [{} {}] has completed execution in {} ms with status {}.",
                    plan.getPlanId(),
                    plan.getScenario().getScenarioInfo().getType(),
                    plan.getEndTime() - plan.getStartTime(),
                    plan.getStatus());
            } catch (Exception e) {
                logger.error("Encountered exception while trying to log plan completion: ", e);
            }

            // Run the next plan instance in queue if available.
            planInstanceQueue.runNextPlanInstance();
        }
    }
}
