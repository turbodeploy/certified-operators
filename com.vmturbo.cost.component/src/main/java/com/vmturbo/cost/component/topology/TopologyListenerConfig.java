package com.vmturbo.cost.component.topology;


import java.util.EnumSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;

/**
 * Setup listener for topologies from Topology Processor.
 *
 */

@Configuration
@Import({TopologyProcessorClientConfig.class})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        return new LiveTopologyEntitiesListener(realtimeTopologyContextId);
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor =
                topologyClientConfig.topologyProcessor(
                    EnumSet.of(Subscription.LiveTopologies));
        topologyProcessor.addLiveTopologyListener(liveTopologyEntitiesListener());
        return topologyProcessor;
    }
}
