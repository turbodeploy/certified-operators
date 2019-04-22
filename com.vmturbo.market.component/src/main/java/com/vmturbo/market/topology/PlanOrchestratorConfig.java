package com.vmturbo.market.topology;

import com.vmturbo.market.runner.MarketRunnerConfig;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.EnumSet;
import java.util.Optional;

/**
 * Configuration for integration with the Plan Orchestrator.
 */
@Configuration
@Import({
    MarketRunnerConfig.class,
    PlanOrchestratorClientConfig.class
})
public class PlanOrchestratorConfig {

    @Autowired
    private MarketRunnerConfig marketRunnerConfig;

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Bean
    public AnalysisCancelListener analysisCancelListener() {
        final AnalysisCancelListener analysisCancelListener = new AnalysisCancelListener(
                marketRunnerConfig.marketRunner());
        planOrchestrator().addPlanListener(analysisCancelListener);
        return analysisCancelListener;
    }

    @Bean
    public PlanOrchestrator planOrchestrator() {
        return planClientConfig.planOrchestrator();
    }
}
