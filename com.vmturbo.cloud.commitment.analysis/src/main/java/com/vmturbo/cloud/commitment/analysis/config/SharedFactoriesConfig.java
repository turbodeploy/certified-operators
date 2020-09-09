package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.cloud.commitment.analysis.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;

/**
 * A configuration for shared factory classes between analysis stages.
 */
@Configuration
public class SharedFactoriesConfig {

    /**
     * The {@link ComputeTierFamilyResolverFactory}.
     * @return The {@link ComputeTierFamilyResolverFactory}.
     */
    @Bean
    public ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory() {
        return new ComputeTierFamilyResolverFactory();
    }
}
