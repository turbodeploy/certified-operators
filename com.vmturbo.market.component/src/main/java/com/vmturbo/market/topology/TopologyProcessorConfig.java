package com.vmturbo.market.topology;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.market.runner.MarketRunnerConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({MarketRunnerConfig.class, TopologyProcessorClientConfig.class})
public class TopologyProcessorConfig {

    private static Logger log = LogManager.getLogger();

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
        final TopologyProcessor topologyProcessor = tpConfig.topologyProcessor();
        topologyProcessor.addEntitiesListener(topologyEntitiesListener());
        return topologyProcessor;
    }
}
