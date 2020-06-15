package com.vmturbo.topology.processor.entity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.EntityInfoREST;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for the entity repository.
 */
@Configuration
@Import({TargetConfig.class, IdentityProviderConfig.class, ClockConfig.class})
public class EntityConfig {

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private TopologyProcessorNotificationSender sender;

    @Value("${validationOldValuesCacheEnabled:true}")
    private boolean oldValuesCacheEnabled;

    @Bean
    public EntityStore entityStore() {
        return new EntityStore(targetConfig.targetStore(),
            identityProviderConfig.identityProvider(),
            sender,
            clockConfig.clock());
    }

    @Bean
    public EntityValidator entityValidator() {
        return new EntityValidator(oldValuesCacheEnabled);
    }

    @Bean
    public EntityRpcService entityInfoRpcService() {
        return new EntityRpcService(entityStore(), targetConfig.targetStore());
    }

    @Bean
    public EntityInfoREST.EntityServiceController entityInfoServiceController() {
        return new EntityInfoREST.EntityServiceController(entityInfoRpcService());
    }
}
