package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory.DefaultCoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher.CoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.DefaultCoverageEntityMatcher.DefaultCoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ConfigurableCoverageRule.ConfigurableCoverageRuleFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.CloudCommitmentFilterFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * A spring configuration for {@link SupplementalRICoverageAnalysisFactory}.
 */
@Configuration
@Import({
        TopologyProcessorListenerConfig.class,
        ReservedInstanceSpecConfig.class
})
public class SupplementalCoverageAnalysisConfig {

    @Autowired
    private TopologyProcessor topologyProcessor;

    @Autowired
    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    @Autowired
    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    @Autowired
    private BillingFamilyRetrieverFactory billingFamilyRetrieverFactory;

    @Autowired
    private IdentityProvider identityProvider;

    @Autowired
    private ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    @Value("${supplementalRICoverageValidation:false}")
    private boolean supplementalRICoverageValidation;

    @Value("${concurrentSupplementalRICoverageAllocation:true}")
    private boolean concurrentSupplementalRICoverageAllocation;

    /**
     * The {@link ThinTargetCache}.
     * @return The {@link ThinTargetCache}.
     */
    @Bean
    public ThinTargetCache thinTargetCache() {
        return new ThinTargetCache(topologyProcessor);
    }

    /**
     * The {@link CoverageTopologyFactory}.
     * @return The {@link CoverageTopologyFactory}.
     */
    @Bean
    public CoverageTopologyFactory coverageTopologyFactory() {
        return new CoverageTopologyFactory(thinTargetCache());
    }

    /**
     * The {@link CloudCommitmentFilterFactory}.
     * @return The {@link CloudCommitmentFilterFactory}.
     */
    @Bean
    public CloudCommitmentFilterFactory cloudCommitmentFilterFactory() {
        return new CloudCommitmentFilterFactory();
    }

    /**
     * The {@link ComputeCommitmentMatcherFactory}.
     * @return The {@link ComputeCommitmentMatcherFactory}.
     */
    @Bean
    public ComputeCommitmentMatcherFactory computeCommitmentMatcherFactory() {
        return new ComputeCommitmentMatcherFactory();
    }

    /**
     * The {@link ConfigurableCoverageRuleFactory}.
     * @return The {@link ConfigurableCoverageRuleFactory}.
     */
    @Bean
    public ConfigurableCoverageRuleFactory configurableCoverageRuleFactory() {
        return new ConfigurableCoverageRuleFactory(
                cloudCommitmentFilterFactory(),
                computeCommitmentMatcherFactory());
    }

    /**
     * The {@link CoverageEntityMatcherFactory}.
     * @return The {@link CoverageEntityMatcherFactory}.
     */
    @Bean
    public CoverageEntityMatcherFactory coverageEntityMatcherFactory() {
        return new DefaultCoverageEntityMatcherFactory();
    }

    /**
     * The {@link CoverageRulesFactory}.
     * @return The {@link CoverageRulesFactory}.
     */
    @Bean
    public CoverageRulesFactory coverageRulesFactory() {
        return new CoverageRulesFactory(
                configurableCoverageRuleFactory(),
                coverageEntityMatcherFactory());
    }

    /**
     * The {@link CoverageAllocatorFactory}.
     * @return The {@link CoverageAllocatorFactory}.
     */
    @Bean
    public CoverageAllocatorFactory coverageAllocatorFactory() {
        return new DefaultCoverageAllocatorFactory(coverageRulesFactory());
    }

    /**
     * The {@link CloudCommitmentAggregatorFactory}.
     * @return The {@link CloudCommitmentAggregatorFactory}.
     */
    @Bean
    public CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory() {
        return new DefaultCloudCommitmentAggregatorFactory(
                identityProvider,
                computeTierFamilyResolverFactory,
                billingFamilyRetrieverFactory);
    }

    /**
     * The {@link SupplementalRICoverageAnalysisFactory}.
     * @return The {@link SupplementalRICoverageAnalysisFactory}.
     */
    @Lazy
    @Bean
    public SupplementalRICoverageAnalysisFactory supplementalRICoverageAnalysisFactory() {
        return new SupplementalRICoverageAnalysisFactory(
                coverageAllocatorFactory(),
                coverageTopologyFactory(),
                reservedInstanceBoughtStore,
                reservedInstanceSpecStore,
                cloudCommitmentAggregatorFactory(),
                supplementalRICoverageValidation,
                concurrentSupplementalRICoverageAllocation);
    }
}
