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

import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;

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
                statsConfig.liveStatsWriter(),
                historyApiConfig.statsAvailabilityTracker(),
                groupServiceClient(),
                statsConfig.systemLoadHelper());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor = topologyClientConfig.topologyProcessor(
                EnumSet.of(Subscription.LiveTopologies));
        topologyProcessor.addLiveTopologyListener(liveTopologyEntitiesListener());
        return topologyProcessor;
    }

    @Bean
    public GroupServiceBlockingStub groupServiceClient() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

}
