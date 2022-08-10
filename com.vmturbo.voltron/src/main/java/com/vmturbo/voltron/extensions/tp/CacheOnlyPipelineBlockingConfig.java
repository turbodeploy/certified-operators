package com.vmturbo.voltron.extensions.tp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineUnblock;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineUnblockLauncher;

/**
 * A Configuration that replaces PipelineBlockingConfig to provide a CacheDiscoveryModeUnblock
 * to the PipelineUnblockLauncher.
 */
@Configuration
public class CacheOnlyPipelineBlockingConfig {

    @Value("${pipelineUnblockType:discovery}")
    private String pipelineUnblockType;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private CacheOnlyOperationConfig operationConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private CacheOnlyDiscoveryDumperConfig discoveryDumperConfig;

    /**
     * Creates a PipelineUnblockLauncher.
     */
    @Bean
    public PipelineUnblockLauncher pipelineUnblockLauncher() {
        return new PipelineUnblockLauncher(getPipelineUnblock(), targetConfig.targetStore());
    }

    private PipelineUnblock getPipelineUnblock() {
        return new CacheDiscoveryModeUnblock(topologyConfig.pipelineExecutorService(),
                targetConfig.targetStore(),
                targetConfig.derivedTargetParser(),
                probeConfig.probeStore(),
                operationConfig.operationManager(),
                identityProviderConfig.identityProvider(),
                discoveryDumperConfig.cacheOnlyDiscoveryDumper());

    }
}
