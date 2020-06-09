package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Setups the listener for the topology processor
 */
@Configuration
@Import(TopologyProcessorClientConfig.class)
public class TopologyProcessorListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Bean
    public TopologyProcessor topologyProcessor() {
        return topologyClientConfig.topologyProcessor(
                TopologyProcessorSubscription.forTopic(Topic.LiveTopologies),
                // used by ReservedInstanceConfig::thinTargetCache
                TopologyProcessorSubscription.forTopic(Topic.Notifications),
                // used by TopologyInforTracker
                TopologyProcessorSubscription.forTopic(Topic.TopologySummaries));
    }
}
