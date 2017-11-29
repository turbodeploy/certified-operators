package com.vmturbo.topology.processor.stitching;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for stitching classes in the TopologyProcessor.
 */
@Configuration
public class StitchingConfig {

    /**
     * No associated @Import because it adds a circular import dependency.
     */
    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Bean
    public StitchingOperationLibrary stitchingOperationLibrary() {
        return new StitchingOperationLibrary();
    }

    @Bean
    public StitchingOperationStore stitchingOperationStore() {
        return new StitchingOperationStore(stitchingOperationLibrary());
    }

    @Bean
    public PreStitchingOperationLibrary stitchingCalculationStore() {
        return new PreStitchingOperationLibrary();
    }

    @Bean
    public StitchingManager stitchingManager() {
        return new StitchingManager(stitchingOperationStore(), stitchingCalculationStore(),
            probeConfig.probeStore(), targetConfig.targetStore());
    }
}
