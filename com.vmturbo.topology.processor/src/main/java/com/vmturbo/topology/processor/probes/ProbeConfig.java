package com.vmturbo.topology.processor.probes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.ProbeREST.ProbeActionCapabilitiesServiceController;
import com.vmturbo.kvstore.KeyValueStoreConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Spring configuration for the Probe package.
 */
@Configuration
@Import({IdentityProviderConfig.class, StitchingConfig.class, KeyValueStoreConfig.class})
public class ProbeConfig {
    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private KeyValueStoreConfig keyValueStoreConfig;

    @Bean
    public ProbeInfoCompatibilityChecker compatibilityChecker() {
        return new ProbeInfoCompatibilityChecker();
    }

    @Bean
    public ProbeStore probeStore() {
        return new RemoteProbeStore(keyValueStoreConfig.keyValueStore(),
                identityProviderConfig.identityProvider(),
                stitchingConfig.stitchingOperationStore());
    }

    @Bean
    public ProbeActionCapabilitiesRpcService probeActionPoliciesService() {
        return new ProbeActionCapabilitiesRpcService(probeStore());
    }

    @Bean
    public ProbeActionCapabilitiesServiceController actionCapabilitiesServiceController() {
        return new ProbeActionCapabilitiesServiceController(probeActionPoliciesService());
    }
}
