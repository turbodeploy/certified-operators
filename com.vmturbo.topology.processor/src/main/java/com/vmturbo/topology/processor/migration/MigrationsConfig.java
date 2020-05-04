package com.vmturbo.topology.processor.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

@Configuration
@Import({
        IdentityProviderConfig.class,
        ProbeConfig.class,
        StitchingConfig.class,
        TopologyProcessorDBConfig.class,
        KVConfig.class})
public class MigrationsConfig {

    @Autowired
    IdentityProviderConfig identityProviderConfig;

    @Autowired
    ProbeConfig probeConfig;

    @Autowired
    TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Autowired
    StitchingConfig stitchingConfig;

    @Autowired
    KVConfig kvConfig;

    @Autowired
    TargetConfig targetConfig;

    @Bean
    public MigrationsLibrary migrationsList() {
        return new MigrationsLibrary(topologyProcessorDBConfig.dsl(),
                probeConfig.probeStore(), stitchingConfig.historyClient(),
                identityProviderConfig.underlyingStore(),
                identityProviderConfig.identityProvider(),
                kvConfig.keyValueStore(), targetConfig.targetStore(), targetConfig.targetDao(),
                targetConfig.identityStore());
    }
}
