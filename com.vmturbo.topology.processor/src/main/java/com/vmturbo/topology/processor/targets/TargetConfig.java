package com.vmturbo.topology.processor.targets;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;

/**
 * Configuration for the target package.
 */
@Configuration
@Import({ProbeConfig.class, IdentityProviderConfig.class, KVConfig.class})
public class TargetConfig {

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private KVConfig kvConfig;

    @Bean
    public TargetStore targetStore() {
        return new KVBackedTargetStore(
                kvConfig.keyValueStore(),
                identityProviderConfig.identityProvider(),
                probeConfig.probeStore());
    }
}
