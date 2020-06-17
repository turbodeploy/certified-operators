package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.topology.TopologyInfoTracker;
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

    /**
     * Used to track the latest real time topology.
     *
     * @return The topology info tracker.
     */
    @Bean
    public TopologyInfoTracker liveTopologyInfoTracker() {

        final TopologyInfoTracker topologyInfoTracker = new TopologyInfoTracker(
                TopologyInfoTracker.SUCCESSFUL_REALTIME_TOPOLOGY_SUMMARY_SELECTOR,
                10);

        topologyProcessor().addTopologySummaryListener(topologyInfoTracker);
        return topologyInfoTracker;
    }
}
