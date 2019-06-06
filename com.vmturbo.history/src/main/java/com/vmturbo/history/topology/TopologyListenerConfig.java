package com.vmturbo.history.topology;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Set up a Listener for the Topology Processor feed.
 **/
@Configuration
@Import({HistoryApiConfig.class, TopologyProcessorClientConfig.class, GroupClientConfig.class})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        return new LiveTopologyEntitiesListener(
                statsConfig.statsWriteCoordinator(),
                historyApiConfig.statsAvailabilityTracker());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor = topologyClientConfig.topologyProcessor(
            TopologyProcessorSubscription.forTopic(Topic.LiveTopologies));
        topologyProcessor.addLiveTopologyListener(liveTopologyEntitiesListener());
        return topologyProcessor;
    }


}
