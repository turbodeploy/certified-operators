package com.vmturbo.market.topology;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.market.runner.MarketRunnerConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({
    MarketRunnerConfig.class,
    TopologyProcessorClientConfig.class,
    LicenseCheckClientConfig.class
})
public class TopologyProcessorConfig {

    @Autowired
    private MarketRunnerConfig marketRunnerConfig;

    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Value("${maxPlacementIterations}")
    private int maxPlacementIterations;

    @Value("${rightsizeLowerWatermark}")
    private float rightsizeLowerWatermark;

    @Value("${rightsizeUpperWatermark}")
    private float rightsizeUpperWatermark;

    @Bean
    public Optional<Integer> maxPlacementsOverride() {
        return maxPlacementIterations > 0
            ? Optional.of(maxPlacementIterations)
            : Optional.empty();
    }

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        final TopologyEntitiesListener topologyEntitiesListener = new TopologyEntitiesListener(
                marketRunnerConfig.marketRunner(), maxPlacementsOverride(),
                rightsizeLowerWatermark, rightsizeUpperWatermark,
                licenseCheckClientConfig.licenseCheckClient());
        topologyProcessor().addLiveTopologyListener(topologyEntitiesListener);
        topologyProcessor().addPlanTopologyListener(topologyEntitiesListener);
        return topologyEntitiesListener;
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpConfig.topologyProcessor(
            TopologyProcessorSubscription.forTopic(Topic.LiveTopologies),
            TopologyProcessorSubscription.forTopic(Topic.PlanTopologies));
    }
}
