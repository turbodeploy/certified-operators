package com.vmturbo.market.topology;

import java.util.EnumSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.market.runner.MarketRunnerConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({MarketRunnerConfig.class, TopologyProcessorClientConfig.class})
public class TopologyProcessorConfig {

    @Autowired
    private MarketRunnerConfig marketRunnerConfig;

    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        return new TopologyEntitiesListener(marketRunnerConfig.marketRunner());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor = tpConfig.topologyProcessor(EnumSet.of(
                Subscription.LiveTopologies, Subscription.PlanTopologies));
        topologyProcessor.addLiveTopologyListener(topologyEntitiesListener());
        topologyProcessor.addPlanTopologyListener(topologyEntitiesListener());
        return topologyProcessor;
    }
}
