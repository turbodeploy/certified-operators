package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.AllocatedDemandClassifier.AllocatedDemandClassifierFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.CloudTierFamilyMatcher.CloudTierFamilyMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;

/**
 * A configuration for all beans leading to a {@link DemandClassificationFactory}.
 */
@Lazy
@Configuration
public class DemandClassificationConfig {

    /**
     * The {@link AllocatedDemandClassifierFactory}.
     * @return The {@link AllocatedDemandClassifierFactory}.
     */
    @Bean
    public AllocatedDemandClassifierFactory allocatedDemandClassifierFactory() {
        return new AllocatedDemandClassifierFactory();
    }

    /**
     * The {@link CloudTierFamilyMatcherFactory}.
     * @return The {@link CloudTierFamilyMatcherFactory}.
     */
    @Bean
    public CloudTierFamilyMatcherFactory cloudTierFamilyMatcherFactory() {
        return new CloudTierFamilyMatcherFactory();
    }

    /**
     * The {@link DemandClassificationFactory}.
     * @return The {@link DemandClassificationFactory}.
     */
    @Bean
    public DemandClassificationFactory demandClassificationFactory() {
        return new DemandClassificationFactory(
                allocatedDemandClassifierFactory(),
                cloudTierFamilyMatcherFactory());
    }
}
