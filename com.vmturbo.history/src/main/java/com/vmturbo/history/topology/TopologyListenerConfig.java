package com.vmturbo.history.topology;

import java.util.EnumSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;

/**
 * Set up a Listener for the Topology Processor feed.
 **/
@Configuration
@Import({HistoryApiConfig.class, TopologyProcessorClientConfig.class})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        return new LiveTopologyEntitiesListener(
                statsConfig.liveStatsWriter(),
                historyApiConfig.statsAvailabilityTracker());
    }

    @Bean
    public PlanTopologyEntitiesListener planTopologyEntitiesListener() {
        return new PlanTopologyEntitiesListener(
                statsConfig.planStatsWriter(),
                historyApiConfig.statsAvailabilityTracker());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor = topologyClientConfig.topologyProcessor(
                EnumSet.of(Subscription.LiveTopologies, Subscription.PlanTopologies));
        topologyProcessor.addLiveTopologyListener(liveTopologyEntitiesListener());
        topologyProcessor.addPlanTopologyListener(planTopologyEntitiesListener());
        return topologyProcessor;
    }
}
