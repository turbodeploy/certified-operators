package com.vmturbo.topology.processor.stitching;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.stitching.StitchingOperationLibrary;

/**
 * Configuration for stitching classes in the TopologyProcessor.
 */
@Configuration
public class StitchingConfig {
    @Bean
    public StitchingOperationLibrary stitchingOperationLibrary() {
        return new StitchingOperationLibrary();
    }

    @Bean
    public StitchingOperationStore stitchingOperationStore() {
        return new StitchingOperationStore(stitchingOperationLibrary());
    }

    @Bean
    public StitchingManager stitchingManager() {
        return new StitchingManager(stitchingOperationStore());
    }
}
