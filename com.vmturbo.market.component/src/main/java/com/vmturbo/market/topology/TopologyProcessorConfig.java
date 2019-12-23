package com.vmturbo.market.topology;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({
    TopologyProcessorClientConfig.class
})
public class TopologyProcessorConfig {

    @Autowired
    private TopologyProcessorClientConfig tpConfig;
    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpConfig.topologyProcessor(
            TopologyProcessorSubscription.forTopic(Topic.LiveTopologies),
            TopologyProcessorSubscription.forTopic(Topic.PlanTopologies),
            // used by the ThinTargetCache
            TopologyProcessorSubscription.forTopic(Topic.Notifications));
    }

    @Bean
    public ThinTargetCache thinTargetCache() {
        return new ThinTargetCache(topologyProcessor());
    }
}
