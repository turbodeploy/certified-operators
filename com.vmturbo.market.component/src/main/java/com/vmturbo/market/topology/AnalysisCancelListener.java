package com.vmturbo.market.topology;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.market.runner.MarketRunner;
import com.vmturbo.plan.orchestrator.api.PlanListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Handler for analysis state change notification coming in from the Plan Orchestrator
 */
public class AnalysisCancelListener implements PlanListener {
    private final Logger logger = LogManager.getLogger();

    private final MarketRunner marketRunner;

    public AnalysisCancelListener(@Nonnull MarketRunner marketRunner) {
        this.marketRunner = Objects.requireNonNull(marketRunner);
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanDTO.PlanInstance planInstance) {
        // if the status has changed to STOPPED, cancel analysis
        if (planInstance.getStatus() == PlanDTO.PlanInstance.PlanStatus.STOPPED) {
            logger.info("received " + planInstance.getStatus().name() + " message for " + planInstance.getTopologyId());
            marketRunner.stopAnalysis(planInstance);
        }
    }
}
