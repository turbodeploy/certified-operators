package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher.CloudCommitmentSpecMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher.DefaultCloudCommitmentSpecMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecResolver;
import com.vmturbo.cloud.commitment.analysis.spec.RISpecPurchaseFilterFactory;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecMatcher.ReservedInstanceSpecMatcherFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;

/**
 * A configuration for all beans leading to a {@link CloudCommitmentSpecMatcherFactory}.
 */
@Lazy
@Import({
        SharedFactoriesConfig.class
})
@Configuration
public class CloudCommitmentSpecMatcherConfig {

    @Autowired
    private CloudCommitmentSpecResolver<ReservedInstanceSpec> riSpecResolver;

    @Autowired
    private ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    /**
     * The {@link ReservedInstanceSpecMatcherFactory}.
     * @return The {@link ReservedInstanceSpecMatcherFactory}.
     */
    @Bean
    public ReservedInstanceSpecMatcherFactory reservedInstanceSpecMatcherFactory() {
        return new ReservedInstanceSpecMatcherFactory();
    }

    /**
     * The {@link RISpecPurchaseFilterFactory}.
     * @return The {@link RISpecPurchaseFilterFactory}.
     */
    @Bean
    public RISpecPurchaseFilterFactory riSpecPurchaseFilterFactory() {
        return new RISpecPurchaseFilterFactory(riSpecResolver, computeTierFamilyResolverFactory);
    }

    /**
     * The {@link CloudCommitmentSpecMatcherFactory}.
     * @return The {@link CloudCommitmentSpecMatcherFactory}.
     */
    @Bean
    public CloudCommitmentSpecMatcherFactory cloudCommitmentSpecMatcherFactory() {
        return new DefaultCloudCommitmentSpecMatcherFactory(
                reservedInstanceSpecMatcherFactory(),
                riSpecPurchaseFilterFactory());
    }
}
