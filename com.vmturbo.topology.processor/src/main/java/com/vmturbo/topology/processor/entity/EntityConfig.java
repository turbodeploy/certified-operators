package com.vmturbo.topology.processor.entity;

import com.vmturbo.common.protobuf.topology.EntityInfoREST;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.targets.TargetStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

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

    @Bean
    public EntityStore entityStore() {
        return new EntityStore(targetConfig.targetStore(),
            identityProviderConfig.identityProvider(),
            clockConfig.clock());
    }

    @Bean
    public EntityValidator entityValidator() {
        return new EntityValidator();
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
