package com.vmturbo.topology.processor.rpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.DiscoveredGroupREST.DiscoveredGroupServiceController;
import com.vmturbo.topology.processor.group.GroupConfig;

@Configuration
@Import({GroupConfig.class})
public class TopologyProcessorRpcConfig {

    @Autowired
    private GroupConfig groupConfig;

    @Bean
    public DiscoveredGroupRpcService discoveredGroupRpcService() {
        return new DiscoveredGroupRpcService(groupConfig.discoveredGroupUploader());
    }

    @Bean
    public DiscoveredGroupServiceController debugServiceController() {
        return new DiscoveredGroupServiceController(discoveredGroupRpcService());
    }
}
