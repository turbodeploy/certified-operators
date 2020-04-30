package com.vmturbo.market.topology;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.PlanDeleted;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.StatusUpdate;
import com.vmturbo.market.runner.MarketRunner;
import com.vmturbo.plan.orchestrator.api.PlanListener;

/**
 * Handler for analysis state change notification coming in from the Plan Orchestrator.
 */
public class AnalysisCancelListener implements PlanListener {
    private final Logger logger = LogManager.getLogger();

    private final MarketRunner marketRunner;

    public AnalysisCancelListener(@Nonnull MarketRunner marketRunner) {
        this.marketRunner = Objects.requireNonNull(marketRunner);
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final StatusUpdate planInstanceStatus) {
        // if the status has changed to STOPPED, cancel analysis
        if (planInstanceStatus.getNewPlanStatus() == PlanDTO.PlanInstance.PlanStatus.STOPPED) {
            logger.info("Received stop notification for plan {}", planInstanceStatus.getPlanId());
            marketRunner.stopAnalysis(planInstanceStatus.getPlanId());
        }
    }

    @Override
    public void onPlanDeleted(@Nonnull final PlanDeleted planDeleted) {
        if (!PlanDTOUtil.isTerminalStatus(planDeleted.getStatusBeforeDelete())) {
            logger.info("Received plan deletion message: {}", planDeleted);
            marketRunner.stopAnalysis(planDeleted.getPlanId());
        }
    }

}
