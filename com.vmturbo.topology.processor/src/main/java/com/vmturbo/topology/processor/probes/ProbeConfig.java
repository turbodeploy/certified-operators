package com.vmturbo.topology.processor.probes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.identity.IdentityProviderConfig;

/**
 * Spring configuration for the Probe package.
 */
@Configuration
@Import(IdentityProviderConfig.class)
public class ProbeConfig {
    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public ProbeStore probeStore() {
        return new RemoteProbeStore(identityProviderConfig.identityProvider());
    }
}
