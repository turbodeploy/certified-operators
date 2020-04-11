package com.vmturbo.plan.orchestrator.market;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.plan.orchestrator.GlobalConfig;

/**
 * Configuration for the part of the plan orchestrator that listens to projected topology
 * notifications from the market.
 */
@Configuration
@Import({
    GlobalConfig.class
})
public class PlanOrchestratorMarketConfig {

    @Autowired
    private GlobalConfig globalConfig;

    /**
     * The {@link PlanProjectedTopologyListener} bean.
     *
     * @return The {@link PlanProjectedTopologyListener}.
     */
    @Bean
    public PlanProjectedTopologyListener planProjectedTopologyListener() {
        final PlanProjectedTopologyListener projectedTopologyListener = new PlanProjectedTopologyListener();
        globalConfig.marketNotificationClient().addProjectedTopologyListener(projectedTopologyListener);
        return projectedTopologyListener;
    }
}
