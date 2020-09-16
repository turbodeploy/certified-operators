package com.vmturbo.plan.orchestrator.plan;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;

public class PlanInstanceCompletionListener implements PlanStatusListener {

    private final Logger logger = LogManager.getLogger();

    private final PlanInstanceQueue planInstanceQueue;

    private final PlanRpcService planRpcService;

    /**
     * Creates a new listener.
     *
     * @param planInstanceQueue Queue of plans to run.
     * @param planRpcService Service related to plan.
     */
    public PlanInstanceCompletionListener(PlanInstanceQueue planInstanceQueue,
            @Nonnull final PlanRpcService planRpcService) {
        this.planInstanceQueue = planInstanceQueue;
        this.planRpcService = planRpcService;
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan)
            throws PlanStatusListenerException {
        if (PlanDTOUtil.isTerminalStatus(plan.getStatus())) {
            try {
                logger.info("Plan [{} {}] has completed execution in {} ms with status {}.",
                    plan.getPlanId(),
                    plan.getScenario().getScenarioInfo().getType(),
                    plan.getEndTime() - plan.getStartTime(),
                    plan.getStatus());
                if (plan.getStatus() == PlanStatus.SUCCEEDED) {
                    planRpcService.onPlanCompletionSuccess(plan);
                }
            } catch (Exception e) {
                logger.error("Encountered exception while trying to log plan " +
                    plan.getPlanId() + " completion: ", e);
            }
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
