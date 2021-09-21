package com.vmturbo.api.component.rpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.mapper.MapperConfig;
import com.vmturbo.api.component.rpc.service.ApiMessageService;

/**
 * Spring configuration that sets up action related protobuf service implementations.
 */
@Configuration
@Import({MapperConfig.class, CommunicationConfig.class})
public class RpcConfig {
    @Autowired
    private MapperConfig mapperConfig;

    @Autowired
    private CommunicationConfig communicationConfig;

    /**
     * The service for message conversion.
     *
     * @return the service for message conversion.
     */
    @Bean
    public ApiMessageService apiMessageService() {
        return new ApiMessageService(mapperConfig.actionSpecMapper(),
                communicationConfig.getRealtimeTopologyContextId());
    }
}
