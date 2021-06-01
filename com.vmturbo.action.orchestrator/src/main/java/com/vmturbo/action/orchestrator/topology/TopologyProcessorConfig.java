package com.vmturbo.action.orchestrator.topology;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Configuration for integration with the {@link TopologyProcessor}.
 */
@Configuration
@Import({TopologyProcessorClientConfig.class})
public class TopologyProcessorConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyProcessorClientConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    /**
     * Get the realtime topology contextId.
     *
     * @return the realtime topology contextId
     */
    @Bean
    public long realtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    /**
     * Configuration for {@link TopologyProcessor}.
     *
     * @return {@link TopologyProcessor}
     */
    @Bean
    public TopologyProcessor topologyProcessor() {
        return
            topologyProcessorClientConfig.topologyProcessor(
                TopologyProcessorSubscription.forTopic(Topic.Notifications),
                TopologyProcessorSubscription.forTopic(Topic.LiveTopologies),
                TopologyProcessorSubscription.forTopic(Topic.EntitiesWithNewState),
                TopologyProcessorSubscription.forTopic(Topic.ExternalActionApprovalResponse),
                TopologyProcessorSubscription.forTopic(Topic.ExternalActionStateChange));
    }

    /**
     * The gRPC channel to the Topology Processor.
     *
     * @return The gRPC channel
     */
    @Bean
    public Channel topologyProcessorChannel() {
        return topologyProcessorClientConfig.topologyProcessorChannel();
    }


    /**
     * See: {@link ActionTopologyStore}.
     *
     * @return The {@link ActionTopologyStore}.
     */
    @Bean
    public ActionTopologyStore actionTopologyStore() {
        return new ActionTopologyStore();
    }
}