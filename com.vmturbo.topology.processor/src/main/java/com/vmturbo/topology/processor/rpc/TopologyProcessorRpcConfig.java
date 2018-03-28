package com.vmturbo.topology.processor.rpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.DiscoveredGroupREST.DiscoveredGroupServiceController;
import com.vmturbo.common.protobuf.topology.TopologyDTOREST;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;
import com.vmturbo.topology.processor.topology.TopologyRpcService;

@Configuration
@Import({
    GroupConfig.class,
    EntityConfig.class,
    IdentityProviderConfig.class,
    ClockConfig.class,
    TopologyConfig.class,
    SchedulerConfig.class
})
public class TopologyProcessorRpcConfig {

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Bean
    public DiscoveredGroupRpcService discoveredGroupRpcService() {
        return new DiscoveredGroupRpcService(groupConfig.discoveredGroupUploader());
    }

    @Bean
    public DiscoveredGroupServiceController debugServiceController() {
        return new DiscoveredGroupServiceController(discoveredGroupRpcService());
    }

    @Bean
    public TopologyRpcService topologyRpcService() {
        return new TopologyRpcService(topologyConfig.topologyHandler(),
            topologyConfig.topologyPipelineFactory(),
            identityProviderConfig.identityProvider(),
            entityConfig.entityStore(),
            schedulerConfig.scheduler(),
            topologyConfig.realtimeTopologyContextId(),
            clockConfig.clock());
    }

    @Bean
    public TopologyDTOREST.TopologyServiceController topologyServiceController() {
        return new TopologyDTOREST.TopologyServiceController(topologyRpcService());
    }
}
