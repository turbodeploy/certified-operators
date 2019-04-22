package com.vmturbo.market;

import javax.annotation.PostConstruct;

import com.vmturbo.market.topology.PlanOrchestratorConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.topology.TopologyProcessorConfig;

/**
 * Component wrapping the Market implementation.
 */
@Configuration("theComponent")
@Import({
    MarketGlobalConfig.class,
    TopologyProcessorConfig.class,
    PlanOrchestratorConfig.class,
    MarketApiConfig.class
})
public class MarketComponent extends BaseVmtComponent {

    @Autowired
    private MarketApiConfig marketApiConfig;

    public static void main(String[] args) {
        startContext(MarketComponent.class);
    }

    @PostConstruct
    private void setup() {
        // add kafka producer health check
        getHealthMonitor().addHealthCheck(marketApiConfig.kafkaHealthMonitor());
    }
}
